import { spawn } from 'node:child_process';
import { setTimeout as delay } from 'node:timers/promises';
import { chromium } from 'playwright';

const host = '127.0.0.1';
const port = 4330;
const origin = `http://${host}:${port}`;
const baseUrl = `${origin}/etl`;
const cleanupTimeoutMs = 5000;

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function withoutFencedCode(markdown) {
  return markdown.replace(/```[\s\S]*?```/g, '');
}

async function waitForPreview() {
  for (let attempt = 0; attempt < 80; attempt += 1) {
    try {
      const response = await fetch(`${baseUrl}/`);
      if (response.ok) return;
    } catch {
      // Keep waiting until the preview server accepts connections.
    }

    await delay(250);
  }

  throw new Error('Docs preview server did not start in time.');
}

async function assertNoHorizontalOverflow(page) {
  const overflow = await page.evaluate(
    () => document.documentElement.scrollWidth - document.documentElement.clientWidth,
  );
  assert(overflow <= 1, `Expected no horizontal overflow, found ${overflow}px.`);
}

async function checkEndpoint(path, expectedContentType) {
  const response = await fetch(`${baseUrl}${path}`);
  assert(response.ok, `${path} returned HTTP ${response.status}.`);
  assert(
    response.headers.get('content-type')?.includes(expectedContentType),
    `${path} did not return ${expectedContentType}.`,
  );
  return response;
}

async function checkSeoEndpoints() {
  const [robots, sitemap, llms, search, socialImage, agentManifest, webManifest, favicon] =
    await Promise.all([
    checkEndpoint('/robots.txt', 'text/plain'),
    checkEndpoint('/sitemap.xml', 'application/xml'),
    checkEndpoint('/llms.txt', 'text/plain'),
    checkEndpoint('/api/search', 'application/json'),
    checkEndpoint('/og/image.png', 'image/png'),
    checkEndpoint('/agents.json', 'application/json'),
    checkEndpoint('/manifest.webmanifest', 'application/manifest+json'),
    checkEndpoint('/assets/etl-favicon.svg', 'image/svg+xml'),
  ]);
  const markdownPaths = [
    '/index.md',
    '/guides/first-pipeline.md',
    '/guides/configure-postgres.md',
    '/guides/custom-implementations.md',
    '/explanation/concepts.md',
    '/explanation/architecture.md',
    '/explanation/schema-changes.md',
    '/explanation/events.md',
    '/explanation/traits.md',
  ];
  const markdownTexts = await Promise.all(
    markdownPaths.map(async (path) => (await checkEndpoint(path, 'text/markdown')).text()),
  );

  assert((await robots.text()).includes('/etl/sitemap.xml'), 'robots.txt does not name the sitemap.');
  assert((await sitemap.text()).includes('/etl/guides/first-pipeline'), 'Sitemap is missing a guide.');
  const llmsText = await llms.text();
  assert(llmsText.includes('Supabase ETL'), 'llms.txt is missing the product name.');
  assert(llmsText.includes('/etl/guides/first-pipeline.md'), 'llms.txt is missing stable Markdown URLs.');
  assert(!llmsText.includes('/llms.mdx/'), 'llms.txt exposes an internal generation route.');
  assert((await search.json()).type === 'advanced', 'Static search index is malformed.');
  assert((await socialImage.arrayBuffer()).byteLength > 0, 'Social image is empty.');
  assert((await favicon.arrayBuffer()).byteLength > 0, 'Favicon image is empty.');

  const manifest = await webManifest.json();
  assert(
    manifest.icons[0].src === '/etl/assets/etl-favicon.svg',
    'Web manifest does not use the ETL logo.',
  );

  const agentData = await agentManifest.json();
  assert(agentData.schema_version === '1.0', 'Agent manifest schema is missing.');
  assert(agentData.pages.length === 9, 'Agent manifest does not include every documentation page.');
  assert(
    agentData.terminology.replication_phases[1] === 'ongoing replication',
    'Agent manifest uses the wrong phase terminology.',
  );
  assert(
    agentData.terminology.primary_verb === 'replicate',
    'Agent manifest does not identify replication as the primary action.',
  );

  const homeText = markdownTexts[0];
  assert(homeText.includes('## Documentation map'), 'Homepage Markdown lacks its documentation map.');
  assert(
    homeText.includes('## Replication phases') && homeText.includes('Streaming describes a transfer mode'),
    'Homepage Markdown does not explain the two-phase replication terminology.',
  );
  assert(!homeText.includes('<div'), 'Homepage Markdown contains presentation-only HTML.');
  assert(!homeText.includes('PipelinesMark'), 'Homepage Markdown contains a UI component name.');
  assert(
    markdownTexts.every((text) => text.includes('Canonical HTML:')),
    'A per-page Markdown file lacks canonical provenance.',
  );
  assert(
    markdownTexts.every(
      (text) => !text.includes('](/guides/') && !text.includes('](/explanation/'),
    ),
    'Agent-readable Markdown contains a base-path-breaking internal link.',
  );
  assert(
    markdownTexts.every(
      (text) =>
        !/^\*\*\d+\s+(minutes?|hours?)\.\*\*/im.test(withoutFencedCode(text)) &&
        !/\b\d+[- ](?:minute|hour)(?:s)?\b/i.test(withoutFencedCode(text)),
    ),
    'A documentation page still contains an editorial duration estimate.',
  );
  assert(
    markdownTexts.every((text) => !text.includes('className="fd-step')),
    'Agent-readable Markdown contains presentation-only step markup.',
  );
  assert(
    markdownTexts.every((text) => !text.includes('<Callout')),
    'Agent-readable Markdown contains presentation-only callout markup.',
  );
}

async function crawlInternalLinks(page) {
  const routes = [
    '/',
    '/guides/first-pipeline/',
    '/guides/configure-postgres/',
    '/guides/custom-implementations/',
    '/explanation/concepts/',
    '/explanation/architecture/',
    '/explanation/events/',
    '/explanation/schema-changes/',
    '/explanation/traits/',
  ];
  const links = new Set(routes.map((route) => `${baseUrl}${route}`));

  for (const route of routes) {
    await page.goto(`${baseUrl}${route}`, { waitUntil: 'load' });
    const hrefs = await page.locator('a[href^="/etl/"]').evaluateAll((anchors) =>
      anchors.map((anchor) => anchor.href.split('#')[0]),
    );
    for (const href of hrefs) links.add(href);
  }

  const failures = [];
  for (const href of links) {
    const response = await fetch(href);
    if (!response.ok) failures.push(`${response.status} ${href}`);
  }
  assert(failures.length === 0, `Broken internal links:\n${failures.join('\n')}`);
}

async function checkDesktop(page) {
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.goto(`${baseUrl}/`, { waitUntil: 'networkidle' });
  await assertNoHorizontalOverflow(page);

  assert((await page.locator('.doc-path-card').count()) === 6, 'Expected six homepage path cards.');
  assert(
    (await page.locator('.doc-path-symbol svg').count()) === 6,
    'Every homepage path card should use a vector icon.',
  );
  const iconSizes = await page.locator('.doc-path-symbol').evaluateAll((icons) =>
    icons.map((icon) => {
      const rect = icon.getBoundingClientRect();
      return [rect.width, rect.height];
    }),
  );
  assert(
    iconSizes.every(([width, height]) => width === iconSizes[0][0] && height === iconSizes[0][1]),
    'Homepage path-card icon containers are not uniformly sized.',
  );
  assert(
    (await page.locator('.etl-brand:visible').first().innerText()) === 'Supabase ETL',
    'The documentation navigation does not use the full Supabase ETL name.',
  );
  const repositoryLabel = await page
    .locator('#nd-sidebar a[aria-label="GitHub"]')
    .evaluate((link) => getComputedStyle(link, '::after').content.replaceAll('"', ''));
  assert(repositoryLabel === 'Supabase ETL', 'The repository footer link is not labeled Supabase ETL.');
  const pipelinesLink = page.locator('#nd-sidebar .etl-sidebar-pipelines');
  assert(
    (await pipelinesLink.innerText()).includes('Using Supabase?\nTry Supabase Pipelines'),
    'The Supabase-focused Pipelines footer link is missing.',
  );
  const pipelinesPosition = await pipelinesLink.evaluate((link) => {
    const sidebar = document.querySelector('#nd-sidebar');
    const github = sidebar?.querySelector('a[aria-label="GitHub"]');
    if (!(sidebar instanceof HTMLElement) || !(github instanceof HTMLElement)) return null;
    const sidebarRect = sidebar.getBoundingClientRect();
    const linkRect = link.getBoundingClientRect();
    const githubRect = github.getBoundingClientRect();
    return {
      distanceFromBottom: sidebarRect.bottom - linkRect.bottom,
      aboveGitHub: linkRect.bottom < githubRect.top,
    };
  });
  assert(
    pipelinesPosition !== null &&
      pipelinesPosition.distanceFromBottom <= 90 &&
      pipelinesPosition.aboveGitHub,
    'Supabase Pipelines is not positioned above GitHub in the sidebar footer.',
  );
  const sidebarText = await page.locator('#nd-sidebar').innerText();
  for (const label of [
    'Home',
    'Get started',
    'First Pipeline',
    'Guides',
    'Configure Postgres',
    'Custom Implementations',
    'Concepts',
    'Logical Replication',
    'Architecture',
    'Schema Changes',
    'Reference',
    'Events',
    'Extension Points',
  ]) {
    assert(sidebarText.includes(label), `Sidebar is missing the concise label: ${label}.`);
  }
  for (const oldLabel of [
    'Build Your First ETL Pipeline',
    'Configure Postgres for Replication',
    'Postgres Logical Replication Concepts',
    'Supabase ETL Architecture',
    'Event Types',
  ]) {
    assert(!sidebarText.includes(oldLabel), `Sidebar still contains the long label: ${oldLabel}.`);
  }
  assert(
    (await page.locator('h1').innerText()) === 'Postgres replication for Rust.',
    'Homepage heading changed unexpectedly.',
  );
  assert(
    (await page.locator('link[rel="canonical"]').getAttribute('href')) ===
      'https://supabase.github.io/etl/',
    'Homepage canonical URL is incorrect.',
  );
  assert(
    (await page.locator('script[type="application/ld+json"]').count()) === 1,
    'Homepage structured data is missing.',
  );
  assert(
    (await page.locator('link[rel="icon"]').getAttribute('href')) ===
      '/etl/assets/etl-favicon.svg',
    'Homepage favicon is incorrect.',
  );
  assert(
    (await page.locator('link[rel="alternate"][type="text/markdown"]').getAttribute('href')) ===
      'https://supabase.github.io/etl/index.md',
    'Homepage does not advertise its agent-readable Markdown.',
  );
  assert(
    (await page.locator('link[rel="alternate"][type="text/plain"]').count()) === 2,
    'Global LLM discovery links are missing.',
  );
  assert(
    (await page.locator('link[rel="alternate"][type="application/json"]').count()) === 1,
    'Agent manifest discovery link is missing.',
  );
  const actionBarSpacing = await page.locator('.etl-page-actions').evaluate((actions) => {
    const firstControl = actions.firstElementChild;
    if (!(firstControl instanceof HTMLElement)) return null;
    const actionsRect = actions.getBoundingClientRect();
    const controlRect = firstControl.getBoundingClientRect();
    return {
      top: controlRect.top - actionsRect.top,
      bottom: actionsRect.bottom - controlRect.bottom,
    };
  });
  assert(
    actionBarSpacing !== null && Math.abs(actionBarSpacing.top - actionBarSpacing.bottom) <= 2,
    'The page-action dividers do not have even vertical spacing.',
  );
  const pageNavigationGap = await page.evaluate(() => {
    const content = document.querySelector('#nd-page > .prose');
    const navigation = content?.nextElementSibling;
    if (!(content instanceof HTMLElement) || !(navigation instanceof HTMLElement)) return 0;
    return navigation.getBoundingClientRect().top - content.getBoundingClientRect().bottom;
  });
  assert(
    pageNavigationGap >= 32,
    `Expected at least 32px above the page navigation, found ${pageNavigationGap}px.`,
  );

  await page.getByRole('button', { name: 'Search Supabase ETL documentation' }).click();
  const searchInput = page.getByRole('textbox', { name: 'Search Supabase ETL documentation' });
  await searchInput.fill('replica identity');
  await page.getByRole('button', { name: /Logical Replication/ }).first().waitFor();
  await searchInput.press('Escape');

  await page.goto(`${baseUrl}/guides/first-pipeline/`, { waitUntil: 'networkidle' });
  await page.getByRole('button', { name: 'Open', exact: true }).click();
  const viewMarkdownLink = page.getByRole('link', { name: /View as Markdown/ });
  const viewMarkdownHref = await viewMarkdownLink.getAttribute('href');
  assert(
    viewMarkdownHref === '/etl/guides/first-pipeline.md',
    'View as Markdown does not use the deployment-aware page URL.',
  );
  const openMarkdownResponse = await page.request.get(new URL(viewMarkdownHref, origin).toString());
  assert(
    openMarkdownResponse.ok() &&
      openMarkdownResponse.headers()['content-type']?.includes('text/markdown'),
    'View as Markdown does not resolve to generated Markdown.',
  );
  await page.keyboard.press('Escape');

  const copyMarkdownResponsePromise = page.waitForResponse(
    (response) => response.url() === `${baseUrl}/guides/first-pipeline.md`,
  );
  await page.getByRole('button', { name: 'Copy Markdown' }).click();
  const copyMarkdownResponse = await copyMarkdownResponsePromise;
  assert(
    copyMarkdownResponse.ok() &&
      copyMarkdownResponse.headers()['content-type']?.includes('text/markdown'),
    'Copy Markdown does not fetch the generated Markdown page.',
  );

  assert(
    (await page.locator('.fd-step').count()) === 5,
    'First Pipeline does not use the native Fumadocs step layout.',
  );
  const codeBlockSurface = await page.locator('figure.shiki').first().evaluate((figure) => {
    const region = figure.querySelector('[role="region"]');
    const pre = figure.querySelector('pre');
    const figureRect = figure.getBoundingClientRect();
    const regionRect = region?.getBoundingClientRect();
    const preStyle = pre ? getComputedStyle(pre) : null;

    return {
      fullWidth: regionRect ? Math.abs(figureRect.width - regionRect.width) <= 2 : false,
      innerRadius: preStyle?.borderRadius,
      innerShadow: preStyle?.boxShadow,
    };
  });
  assert(
    codeBlockSurface.fullWidth &&
      codeBlockSurface.innerRadius === '0px' &&
      codeBlockSurface.innerShadow === 'none',
    'Code blocks do not use one continuous background surface.',
  );
  const tutorialCallout = page.getByText('The example reads the standard', { exact: false });
  await tutorialCallout.waitFor();
  assert(
    (await tutorialCallout.locator('xpath=ancestor::*[contains(@class, "etl-callout")][1]').count()) === 1,
    'The First Pipeline note does not use a native Fumadocs callout.',
  );

  await page.goto(`${baseUrl}/explanation/architecture/`, { waitUntil: 'networkidle' });
  await assertNoHorizontalOverflow(page);
  await page.locator('.mermaid-diagram svg').waitFor({ timeout: 10000 });
  assert(
    (await page.locator('#nd-sidebar a[data-active="true"]').getAttribute('href')) ===
      '/etl/explanation/architecture/',
    'Architecture is not active in the sidebar.',
  );
}

async function checkMobile(page) {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto(`${baseUrl}/`, { waitUntil: 'networkidle' });
  await assertNoHorizontalOverflow(page);
  assert(
    (await page.locator('.doc-path-card').count()) === 6,
    'Mobile homepage is missing path cards.',
  );
  const mobileCardsFit = await page.locator('.doc-path-card').evaluateAll((cards) =>
    cards.every((card) => {
      const rect = card.getBoundingClientRect();
      return rect.left >= 0 && rect.right <= window.innerWidth && card.scrollWidth <= card.clientWidth;
    }),
  );
  assert(mobileCardsFit, 'A mobile homepage card is clipped or overflows the viewport.');

  await page.goto(`${baseUrl}/guides/first-pipeline/`, { waitUntil: 'networkidle' });
  await assertNoHorizontalOverflow(page);

  await page.getByRole('button', { name: 'Open Sidebar' }).click();
  const mobilePipelinesLink = page.locator('#nd-sidebar-mobile .etl-sidebar-pipelines');
  await mobilePipelinesLink.waitFor({ state: 'visible' });
  await page.waitForTimeout(350);
  assert(
    (await mobilePipelinesLink.innerText()).includes('Using Supabase?\nTry Supabase Pipelines'),
    'The mobile sidebar is missing the Pipelines footer link.',
  );
  const mobilePipelinesStyle = await mobilePipelinesLink.evaluate((link) => {
    const footer = link.parentElement;
    const rect = link.getBoundingClientRect();

    return {
      footerBorderTop: footer ? getComputedStyle(footer).borderTopWidth : null,
      fitsViewport: rect.left >= 0 && rect.right <= window.innerWidth,
    };
  });
  assert(
    mobilePipelinesStyle.footerBorderTop === '0px' && mobilePipelinesStyle.fitsViewport,
    'The mobile Pipelines card divider or sizing is incorrect.',
  );
  await page
    .locator('#nd-sidebar-mobile')
    .getByRole('link', { name: 'Architecture', exact: true })
    .click();
  await page.waitForURL(`${baseUrl}/explanation/architecture/`);
  await assertNoHorizontalOverflow(page);

  await page.getByRole('button', { name: 'Open Search' }).click();
  const searchInput = page.getByRole('textbox', { name: 'Search Supabase ETL documentation' });
  await searchInput.fill('schema changes');
  await page.getByRole('button', { name: /Schema Changes/ }).first().waitFor();
  await searchInput.press('Escape');
}

const preview = spawn('npm', ['run', 'preview', '--', '--host', host, '--port', String(port)], {
  detached: process.platform !== 'win32',
  stdio: ['ignore', 'pipe', 'pipe'],
});

preview.stdout.on('data', (chunk) => process.stdout.write(chunk));
preview.stderr.on('data', (chunk) => process.stderr.write(chunk));

let browser;

function signalPreview(signal) {
  if (preview.exitCode !== null || preview.signalCode !== null) return;

  try {
    if (process.platform === 'win32') preview.kill(signal);
    else process.kill(-preview.pid, signal);
  } catch (error) {
    if (error.code !== 'ESRCH') throw error;
  }
}

async function stopPreview() {
  if (preview.exitCode !== null || preview.signalCode !== null) return;

  const closed = new Promise((resolve) => preview.once('close', resolve));
  signalPreview('SIGTERM');

  const stopped = await Promise.race([
    closed.then(() => true),
    delay(cleanupTimeoutMs).then(() => false),
  ]);
  if (stopped) return;

  signalPreview('SIGKILL');
  await Promise.race([closed, delay(cleanupTimeoutMs)]);
}

try {
  await waitForPreview();
  await checkSeoEndpoints();

  browser = await chromium.launch();
  const context = await browser.newContext({ permissions: ['clipboard-read', 'clipboard-write'] });
  const page = await context.newPage();
  const pageErrors = [];
  page.on('pageerror', (error) => pageErrors.push(error.message));

  await checkDesktop(page);
  await checkMobile(page);
  await crawlInternalLinks(page);

  assert(pageErrors.length === 0, `Browser errors:\n${pageErrors.join('\n')}`);
  console.log(
    'Docs smoke checks passed: SEO, agent feeds, page actions, search, diagrams, responsive UI, and links.',
  );
} finally {
  if (browser) await Promise.race([browser.close(), delay(cleanupTimeoutMs)]);
  await stopPreview();
}
