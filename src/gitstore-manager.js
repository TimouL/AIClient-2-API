import 'dotenv/config';
import path from 'path';
import { existsSync, promises as fs } from 'fs';
import { execFile } from 'child_process';
import { promisify } from 'util';

const exec = promisify(execFile);

const DEFAULT_FILES = ['config.json', 'provider_pools.json', 'pwd', 'configs'];
const DEFAULT_BRANCH = process.env.GITSTORE_GIT_BRANCH || 'main';

const REQUIRED_ENV = ['GITSTORE_GIT_URL', 'GITSTORE_GIT_USERNAME', 'GITSTORE_GIT_TOKEN'];

class GitstoreManager {
    constructor() {
        this.baseDir = path.resolve('data');
        this.gitDir = path.join(this.baseDir, 'gitstore');
        this.localDir = this.baseDir;
        this.files = [...DEFAULT_FILES];
        this.branch = DEFAULT_BRANCH;
        this.mode = 'LOCAL'; // ACTIVE | DEGRADED | LOCAL
        this.pending = false;
        this.error = null;
        this.initialized = false;
        this.gitEnv = null;
        this.askPassPath = null;
    }

    getState() {
        return {
            mode: this.mode,
            pending: this.pending,
            error: this.error,
            branch: this.branch
        };
    }

    resolveWorkingPath(relativePath) {
        return path.isAbsolute(relativePath) ? relativePath : path.join(process.cwd(), relativePath);
    }

    resolveLocalPath(relativePath) {
        return path.isAbsolute(relativePath) ? relativePath : path.join(this.localDir, relativePath);
    }

    resolveGitPath(relativePath) {
        return path.isAbsolute(relativePath) ? relativePath : path.join(this.gitDir, relativePath);
    }

    async ensureInitialized(files = []) {
        if (files.length) {
            this.files = Array.from(new Set([...this.files, ...files]));
        }
        if (this.initialized) {
            return this.getState();
        }

        await fs.mkdir(this.baseDir, { recursive: true });
        await fs.mkdir(this.gitDir, { recursive: true });

        const missingEnv = REQUIRED_ENV.filter((key) => !process.env[key]);
        if (missingEnv.length > 0) {
            this.mode = 'LOCAL';
            this.error = `Missing env: ${missingEnv.join(', ')}`;
            this.initialized = true;
            return this.getState();
        }

        try {
            this.gitEnv = await this._prepareGitEnv();
            await this._prepareRepository();
            this.mode = 'ACTIVE';
            this.pending = false;
            this.error = null;
        } catch (err) {
            this.mode = 'LOCAL';
            this.error = err?.message || String(err);
        }

        this.initialized = true;
        return this.getState();
    }

    async ensureWorkingCopies(files = this.files) {
        await this.ensureInitialized(files);

        if (this.mode !== 'LOCAL') {
            await this._pullLatest().catch((err) => {
                this.mode = 'DEGRADED';
                this.error = err?.message || String(err);
                this.pending = true;
            });
        }

        for (const relativePath of files) {
            const workingPath = this.resolveWorkingPath(relativePath);
            const localPath = this.resolveLocalPath(relativePath);
            const gitPath = this.resolveGitPath(relativePath);

            const source = await this._pickSource({ gitPath, localPath, relativePath });
            if (!source) {
                continue;
            }

            const sourceStat = await fs.stat(source);
            const isDir = sourceStat.isDirectory();

            if (isDir) {
                await this._copyPath(source, workingPath);
                await this._copyPath(source, localPath);
                if (this.mode !== 'LOCAL') {
                    await this._copyPath(source, gitPath);
                }
            } else {
                const content = await fs.readFile(source);
                await this._writeFile(workingPath, content);
                await this._writeFile(localPath, content);
                if (this.mode !== 'LOCAL') {
                    await this._writeFile(gitPath, content);
                }
            }
        }

        return this.getState();
    }

    async readJson(relativePath) {
        await this.ensureWorkingCopies([relativePath]);
        const targetPath = this.resolveWorkingPath(relativePath);
        const data = await fs.readFile(targetPath, 'utf8');
        return JSON.parse(data);
    }

    async writeJson(relativePath, data) {
        await this.ensureInitialized([relativePath]);
        const content = typeof data === 'string' ? data : JSON.stringify(data, null, 2);

        const workingPath = this.resolveWorkingPath(relativePath);
        const localPath = this.resolveLocalPath(relativePath);
        const gitPath = this.resolveGitPath(relativePath);

        await this._writeFile(workingPath, content);
        await this._writeFile(localPath, content);

        if (this.mode === 'LOCAL') {
            this.pending = false;
            return this.getState();
        }

        await this._writeFile(gitPath, content);
        await this._stageFiles([relativePath]);

        const hasChanges = await this._hasStagedChanges();

        if (hasChanges) {
            await this._commit();
        }

        const pushed = await this._pushWithRetry(hasChanges || this.pending);
        if (!pushed) {
            this.mode = 'DEGRADED';
            this.pending = true;
        } else {
            this.mode = 'ACTIVE';
            this.pending = false;
            this.error = null;
        }

        return this.getState();
    }

    async _writeFile(filePath, content) {
        await fs.mkdir(path.dirname(filePath), { recursive: true });
        const encoding = Buffer.isBuffer(content) ? undefined : 'utf8';
        await fs.writeFile(filePath, content, encoding);
    }

    async _copyPath(fromPath, toPath) {
        await fs.mkdir(path.dirname(toPath), { recursive: true });
        await fs.cp(fromPath, toPath, { recursive: true });
    }

    async _prepareGitEnv() {
        const askPassDir = await fs.mkdtemp(path.join(this.baseDir, 'gitstore-askpass-'));
        const askPassPath = path.join(askPassDir, 'askpass.sh');
        const script = [
            '#!/bin/sh',
            'case "$1" in',
            "*Username*) echo \"$GITSTORE_GIT_USERNAME\" ;;",
            "*) echo \"$GITSTORE_GIT_TOKEN\" ;;",
            'esac'
        ].join('\n');
        await fs.writeFile(askPassPath, script, { mode: 0o700 });
        this.askPassPath = askPassPath;
        return {
            ...process.env,
            GIT_ASKPASS: askPassPath,
            GIT_TERMINAL_PROMPT: '0',
            GITSTORE_GIT_USERNAME: process.env.GITSTORE_GIT_USERNAME,
            GITSTORE_GIT_TOKEN: process.env.GITSTORE_GIT_TOKEN
        };
    }

    async _prepareRepository() {
        const hasGit = existsSync(path.join(this.gitDir, '.git'));
        if (!hasGit) {
            const dirEntries = await fs.readdir(this.gitDir);
            if (dirEntries.length === 0) {
                await this._runGit(['clone', process.env.GITSTORE_GIT_URL, this.gitDir], process.cwd());
            } else {
                await this._runGit(['init'], this.gitDir);
                await this._runGit(['remote', 'add', 'origin', process.env.GITSTORE_GIT_URL], this.gitDir).catch(() => {});
            }
        }

        await this._runGit(['fetch', 'origin'], this.gitDir).catch(() => {});
        await this._checkoutBranch();
        await this._pullLatest().catch(() => {});
    }

    async _checkoutBranch() {
        try {
            await this._runGit(['checkout', this.branch], this.gitDir);
        } catch (err) {
            await this._runGit(['checkout', '-b', this.branch], this.gitDir);
        }
    }

    async _pullLatest() {
        await this._runGit(['pull', 'origin', this.branch, '--rebase'], this.gitDir);
    }

    async _stageFiles(files) {
        await this._runGit(['add', ...files], this.gitDir);
    }

    async _hasStagedChanges() {
        try {
            await this._runGit(['diff', '--cached', '--quiet'], this.gitDir);
            return false;
        } catch (err) {
            return true;
        }
    }

    async _commit() {
        const hasHead = await this._hasHead();
        if (hasHead) {
            await this._runGit(['commit', '--amend', '--no-edit', '--allow-empty'], this.gitDir);
        } else {
            await this._runGit(['commit', '-m', 'gitstore: init data'], this.gitDir);
        }
    }

    async _pushWithRetry(shouldPush) {
        if (!shouldPush) {
            return true;
        }
        const attempts = 3;
        for (let i = 0; i < attempts; i++) {
            try {
                await this._runGit(['push', 'origin', this.branch, '--force'], this.gitDir);
                return true;
            } catch (err) {
                this.error = err?.message || String(err);
                await new Promise((resolve) => setTimeout(resolve, 3000));
            }
        }
        return false;
    }

    async _hasHead() {
        try {
            await this._runGit(['rev-parse', '--verify', 'HEAD'], this.gitDir);
            return true;
        } catch (err) {
            return false;
        }
    }

    async _pickSource({ gitPath, localPath, relativePath }) {
        if (this.mode !== 'LOCAL' && existsSync(gitPath)) {
            return gitPath;
        }
        if (existsSync(localPath)) {
            return localPath;
        }
        const examplePath = `${this.resolveWorkingPath(relativePath)}.example`;
        if (existsSync(examplePath)) {
            return examplePath;
        }
        return null;
    }

    async _runGit(args, cwd) {
        await exec('git', args, { cwd, env: this.gitEnv });
    }

    async syncDirectory(relativeDir) {
        await this.ensureInitialized([relativeDir]);
        await this.ensureWorkingCopies([relativeDir]);

        const workingPath = this.resolveWorkingPath(relativeDir);
        const localPath = this.resolveLocalPath(relativeDir);
        const gitPath = this.resolveGitPath(relativeDir);

        if (!existsSync(workingPath)) {
            return this.getState();
        }

        // Mirror to local copy
        await this._copyPath(workingPath, localPath);

        if (this.mode === 'LOCAL') {
            this.pending = false;
            return this.getState();
        }

        await this._copyPath(workingPath, gitPath);
        await this._stageFiles([relativeDir]);

        const hasChanges = await this._hasStagedChanges();
        if (hasChanges) {
            await this._commit();
        }

        const pushed = await this._pushWithRetry(hasChanges || this.pending);
        if (!pushed) {
            this.mode = 'DEGRADED';
            this.pending = true;
        } else {
            this.mode = 'ACTIVE';
            this.pending = false;
            this.error = null;
        }

        return this.getState();
    }
}

const gitstoreManager = new GitstoreManager();

export function getGitstoreState() {
    return gitstoreManager.getState();
}

export function resolveStorePath(relativePath) {
    return gitstoreManager.resolveWorkingPath(relativePath);
}

export async function ensureGitstoreInitialized(files) {
    return gitstoreManager.ensureInitialized(files);
}

export async function ensureGitstoreWorkingCopies(files) {
    return gitstoreManager.ensureWorkingCopies(files);
}

export async function readJsonFromStore(relativePath) {
    return gitstoreManager.readJson(relativePath);
}

export async function writeJsonToStore(relativePath, data) {
    return gitstoreManager.writeJson(relativePath, data);
}

export function getGitstoreManager() {
    return gitstoreManager;
}

export async function syncDirectoryInStore(relativeDir) {
    return gitstoreManager.syncDirectory(relativeDir);
}

export async function syncAllInStore({
    configPath = 'config.json',
    configData,
    providerPoolsPath = 'provider_pools.json',
    providerPoolsData = {},
    includeConfigsDir = true,
    includePwd = true
} = {}) {
    await gitstoreManager.ensureInitialized([configPath, providerPoolsPath]);
    const targets = [configPath, providerPoolsPath];
    if (includePwd) targets.push('pwd');
    if (includeConfigsDir) targets.push('configs');
    await gitstoreManager.ensureWorkingCopies(targets);

    try {
        await gitstoreManager.writeJson(configPath, configData ?? {});
        await gitstoreManager.writeJson(providerPoolsPath, providerPoolsData ?? {});
        if (includeConfigsDir) {
            try {
                await fs.access('configs');
                await gitstoreManager.syncDirectory('configs');
            } catch (err) {
                // configs 不存在时忽略
            }
        }
        if (includePwd && existsSync('pwd')) {
            const pwdContent = await fs.readFile('pwd', 'utf8');
            await gitstoreManager.writeJson('pwd', pwdContent);
        }
    } catch (err) {
        gitstoreManager.mode = gitstoreManager.mode === 'ACTIVE' ? 'DEGRADED' : gitstoreManager.mode;
        gitstoreManager.error = err?.message || String(err);
    }

    return gitstoreManager.getState();
}
