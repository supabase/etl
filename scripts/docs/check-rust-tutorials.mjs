import { execFileSync } from 'node:child_process';
import { existsSync, mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';

const repositoryRoot = resolve(import.meta.dirname, '../..');
const etlCratePath = join(repositoryRoot, 'crates/etl');
const targetDirectory = join(repositoryRoot, 'target/docs-tutorials');

const tutorials = [
  {
    name: 'First Pipeline',
    source: 'docs/src/content/docs/guides/first-pipeline.mdx',
    files: ['Cargo.toml', 'src/main.rs'],
  },
  {
    name: 'Custom Implementations',
    source: 'docs/src/content/docs/guides/custom-implementations.mdx',
    files: ['Cargo.toml', 'src/custom_store.rs', 'src/http_destination.rs', 'src/main.rs'],
  },
];

function extractTitledBlocks(markdown) {
  const files = new Map();
  const blockPattern = /^```(?:toml|rust)[^\n]*\btitle="([^"]+)"[^\n]*\n([\s\S]*?)^```/gm;

  for (const match of markdown.matchAll(blockPattern)) files.set(match[1], match[2]);
  return files;
}

function withLocalEtlDependency(manifest) {
  const localDependency = `etl = { path = ${JSON.stringify(etlCratePath)} }`;
  const updated = manifest.replace(
    /etl\s*=\s*\{\s*git\s*=\s*"https:\/\/github\.com\/supabase\/etl"\s*\}/,
    localDependency,
  );

  if (updated === manifest) throw new Error('Tutorial manifest is missing the expected ETL dependency.');
  return updated;
}

function completeManifest(manifest, crateName) {
  const packageSection = `[package]\nname = "${crateName}"\nversion = "0.1.0"\nedition = "2024"\n\n`;
  const complete = manifest.includes('[package]') ? manifest : packageSection + manifest;
  return withLocalEtlDependency(complete);
}

function checkTutorial(tutorial, temporaryRoot) {
  const sourcePath = join(repositoryRoot, tutorial.source);
  const blocks = extractTitledBlocks(readFileSync(sourcePath, 'utf8'));
  const crateDirectory = join(temporaryRoot, tutorial.name.toLowerCase().replaceAll(' ', '-'));

  for (const file of tutorial.files) {
    const contents = blocks.get(file);
    if (contents === undefined) throw new Error(`${tutorial.name} is missing the ${file} code block.`);

    const destination = join(crateDirectory, file);
    mkdirSync(dirname(destination), { recursive: true });
    writeFileSync(
      destination,
      file === 'Cargo.toml'
        ? completeManifest(contents, tutorial.name.toLowerCase().replaceAll(' ', '-'))
        : contents,
    );
  }

  execFileSync('cargo', ['+1.95.0', 'check'], {
    cwd: crateDirectory,
    env: { ...process.env, CARGO_TARGET_DIR: targetDirectory },
    stdio: 'inherit',
  });

  console.log(`Compiled tutorial: ${tutorial.name}`);
}

const requestedOutputDirectory = process.env.ETL_TUTORIAL_OUTPUT_DIR;
if (requestedOutputDirectory && existsSync(requestedOutputDirectory)) {
  throw new Error(`ETL_TUTORIAL_OUTPUT_DIR already exists: ${requestedOutputDirectory}`);
}
const temporaryRoot = requestedOutputDirectory
  ? resolve(requestedOutputDirectory)
  : mkdtempSync(join(tmpdir(), 'etl-docs-tutorials-'));
if (requestedOutputDirectory) mkdirSync(temporaryRoot, { recursive: true });

try {
  for (const tutorial of tutorials) checkTutorial(tutorial, temporaryRoot);
} finally {
  if (!requestedOutputDirectory) rmSync(temporaryRoot, { recursive: true, force: true });
}

if (requestedOutputDirectory) console.log(`Prepared tutorial crates in ${temporaryRoot}`);
