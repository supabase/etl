import { copyFile, mkdir, readdir } from 'node:fs/promises';
import { dirname, join, relative, resolve, sep } from 'node:path';

const exportDirectory = resolve('out');
const generatedMarkdownDirectory = join(exportDirectory, 'llms.mdx');

async function findMarkdownFiles(directory) {
  const entries = await readdir(directory, { withFileTypes: true });
  const files = [];

  for (const entry of entries) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) files.push(...(await findMarkdownFiles(path)));
    else if (entry.isFile() && entry.name === 'content.md') files.push(path);
  }

  return files;
}

function publicMarkdownPath(sourcePath) {
  const directory = dirname(relative(generatedMarkdownDirectory, sourcePath));
  if (directory === '.') return join(exportDirectory, 'index.md');

  const segments = directory.split(sep);
  const pageName = segments.pop();
  return join(exportDirectory, ...segments, `${pageName}.md`);
}

const markdownFiles = await findMarkdownFiles(generatedMarkdownDirectory);

for (const sourcePath of markdownFiles) {
  const destinationPath = publicMarkdownPath(sourcePath);
  await mkdir(dirname(destinationPath), { recursive: true });
  await copyFile(sourcePath, destinationPath);
}

console.log(`Generated ${markdownFiles.length} stable agent-readable Markdown aliases.`);
