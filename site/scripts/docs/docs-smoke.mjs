import { spawn } from 'node:child_process';
import { readFile, readdir } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { chromium } from 'playwright';

const repositoryRoot = resolve(import.meta.dirname, '../../..');
const host = '127.0.0.1';
const port = 4330;
const origin = `http://${host}:${port}`;
const baseUrl = `${origin}/etl`;
const cleanupTimeoutMs = 5000;

// The public canonical origin this build is deployed under. Overridable via the
// same env vars as `next.config.mjs`/`src/lib/site.ts`, so a fork or preview
// deployment can run this smoke test against its own domain.
const canonicalOrigin = process.env.SITE_ORIGIN ?? 'https://supabase.github.io';
const canonicalBasePath = process.env.SITE_BASE_PATH ?? '/etl';
const canonicalBaseUrl = `${canonicalOrigin}${canonicalBasePath}`;
const projectStatus =
  'Supabase ETL is under active development. APIs and setup steps may change before the first stable release.';

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function withoutFencedCode(markdown) {
  return markdown.replace(/```[\s\S]*?```/g, '');
}

async function checkSourceCodeFences() {
  const documentationDirectory = join(repositoryRoot, 'site/content/docs');
  const documentationFiles = (await readdir(documentationDirectory, { recursive: true }))
    .filter((path) => /\.(?:md|mdx)$/.test(path))
    .map((path) => resolve(documentationDirectory, path));
  const files = [join(repositoryRoot, 'README.md'), ...documentationFiles];
  const allowedLanguages = new Set(['bash', 'ini', 'mermaid', 'rust', 'sql', 'text', 'toml', 'yaml']);
  const titleLanguages = [
    [/title="[^"]+\.rs"/, 'rust'],
    [/title="Cargo\.toml"/, 'toml'],
    [/title="[^"]+\.yaml"/, 'yaml'],
    [/title="Terminal"/, 'bash'],
    [/title="psql"/, 'sql'],
    [/title="postgresql\.conf(?: \(standby\))?"/, 'ini'],
  ];
  const failures = [];

  for (const file of files) {
    const lines = (await readFile(file, 'utf8')).split('\n');
    let inFence = false;

    for (const [index, line] of lines.entries()) {
      if (!line.startsWith('```')) continue;
      if (inFence) {
        inFence = false;
        continue;
      }

      inFence = true;
      const info = line.slice(3).trim();
      const language = info.split(/\s+/, 1)[0];
      if (!allowedLanguages.has(language)) {
        failures.push(`${file}:${index + 1} has missing or unsupported language: ${line}`);
        continue;
      }

      for (const [titlePattern, expectedLanguage] of titleLanguages) {
        if (titlePattern.test(info) && language !== expectedLanguage) {
          failures.push(
            `${file}:${index + 1} uses ${language} for a ${expectedLanguage} code block: ${line}`,
          );
        }
      }
    }
  }

  assert(failures.length === 0, `Invalid documentation code fences:\n${failures.join('\n')}`);
}

async function checkProjectStatusConsistency() {
  const files = [
    join(repositoryRoot, 'README.md'),
    join(repositoryRoot, 'site/content/docs/guides/first-pipeline.mdx'),
    join(repositoryRoot, 'site/content/docs/guides/standalone-replicator.mdx'),
    join(repositoryRoot, 'site/src/lib/site.ts'),
  ];

  for (const file of files) {
    const normalized = (await readFile(file, 'utf8')).replace(/^>\s?/gm, '').replace(/\s+/g, ' ');
    assert(normalized.includes(projectStatus), `${file} does not use the canonical project status.`);
  }

  for (const file of files.slice(1, 3)) {
    const source = await readFile(file, 'utf8');
    assert(
      source.includes('type="info" title="Active development"'),
      `${file} does not present project status as consistent informational guidance.`,
    );
  }
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
    '/guides/standalone-replicator.md',
    '/guides/configure-postgres.md',
    '/guides/custom-implementations.md',
    '/explanation/concepts.md',
    '/explanation/architecture.md',
    '/explanation/schema-changes.md',
    '/reference/destinations.md',
    '/explanation/events.md',
    '/explanation/traits.md',
  ];
  const markdownTexts = await Promise.all(
    markdownPaths.map(async (path) => (await checkEndpoint(path, 'text/markdown')).text()),
  );

  assert((await robots.text()).includes('/etl/sitemap.xml'), 'robots.txt does not name the sitemap.');
  const sitemapText = await sitemap.text();
  assert(
    sitemapText.includes(
      `<loc>${canonicalBaseUrl}/guides/first-pipeline/</loc>`,
    ) &&
      !sitemapText.includes(
        `<loc>${canonicalBaseUrl}/guides/first-pipeline</loc>`,
      ),
    'Sitemap HTML locations do not use canonical trailing slashes.',
  );
  const llmsText = await llms.text();
  assert(llmsText.includes('Supabase ETL'), 'llms.txt is missing the product name.');
  assert(
    llmsText.includes('high-performance Postgres replication engine written in Rust'),
    'llms.txt does not use the canonical product positioning.',
  );
  assert(llmsText.includes('/etl/guides/first-pipeline.md'), 'llms.txt is missing stable Markdown URLs.');
  assert(
    llmsText.includes('/etl/reference/destinations.md'),
    'llms.txt is missing the Destinations reference.',
  );
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
  assert(
    agentData.description.includes('high-performance Postgres replication engine written in Rust'),
    'Agent manifest does not use the canonical product positioning.',
  );
  assert(
    agentData.status_description === projectStatus,
    'Agent manifest does not use the canonical project status.',
  );
  assert(
    agentData.pages.some(
      (page) =>
        page.html_url === `${canonicalBaseUrl}/guides/standalone-replicator/` &&
        page.section === 'Get started',
    ),
    'Standalone Replicator is not grouped under Get started for agents.',
  );
  assert(
    agentData.pages.some(
      (page) =>
        page.html_url === `${canonicalBaseUrl}/reference/destinations/` &&
        page.section === 'Reference',
    ),
    'Destinations is not grouped under Reference for agents.',
  );
  assert(
    agentData.pages.length === markdownPaths.length,
    'Agent manifest does not include every documentation page.',
  );
  assert(
    agentData.terminology.replication_phases[1] === 'ongoing replication',
    'Agent manifest uses the wrong phase terminology.',
  );
  assert(
    agentData.terminology.primary_verb === 'replicate',
    'Agent manifest does not identify replication as the primary action.',
  );
  assert(
    agentData.pages.every(
      (page) =>
        new URL(page.html_url).pathname.endsWith('/') &&
        new URL(page.markdown_url).pathname.endsWith('.md'),
    ),
    'Agent manifest page URLs do not distinguish HTML routes from Markdown files.',
  );
  assert(
    agentData.discovery.search_index === `${canonicalBaseUrl}/api/search` &&
      agentData.discovery.llms_txt === `${canonicalBaseUrl}/llms.txt`,
    'Static agent endpoints received an invalid trailing slash.',
  );

  const homeText = markdownTexts[0];
  assert(
    homeText.includes('high-performance Postgres replication engine written in Rust'),
    'Homepage Markdown does not use the canonical product positioning.',
  );
  assert(homeText.includes('## Documentation map'), 'Homepage Markdown lacks its documentation map.');
  assert(
    homeText.includes('## Replication phases') && homeText.includes('Streaming describes a transfer mode'),
    'Homepage Markdown does not explain the two-phase replication terminology.',
  );
  assert(!homeText.includes('<div'), 'Homepage Markdown contains presentation-only HTML.');
  assert(!homeText.includes('PipelinesMark'), 'Homepage Markdown contains a UI component name.');
  const canonicalHtmlUrls = [
    `${canonicalBaseUrl}/`,
    `${canonicalBaseUrl}/guides/first-pipeline/`,
    `${canonicalBaseUrl}/guides/standalone-replicator/`,
    `${canonicalBaseUrl}/guides/configure-postgres/`,
    `${canonicalBaseUrl}/guides/custom-implementations/`,
    `${canonicalBaseUrl}/explanation/concepts/`,
    `${canonicalBaseUrl}/explanation/architecture/`,
    `${canonicalBaseUrl}/explanation/schema-changes/`,
    `${canonicalBaseUrl}/reference/destinations/`,
    `${canonicalBaseUrl}/explanation/events/`,
    `${canonicalBaseUrl}/explanation/traits/`,
  ];
  assert(
    markdownTexts.every((text, index) =>
      text.includes(`Canonical HTML: ${canonicalHtmlUrls[index]}`),
    ),
    'A per-page Markdown file lacks its canonical trailing-slash HTML URL.',
  );
  assert(
    markdownTexts.every(
      (text) =>
        !text.includes('](/guides/') &&
        !text.includes('](/explanation/') &&
        !text.includes('](/reference/'),
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
  const destinationsText = markdownTexts[8];
  assert(
    destinationsText.includes('**Status: Stable**') &&
      destinationsText.match(/\*\*Status: In progress\*\*/g)?.length === 3 &&
      destinationsText.includes('**Status: Deprecated**') &&
      !destinationsText.includes('<DestinationStatus'),
    'The Destinations Markdown does not expose consistent plain-text statuses.',
  );
  assert(
    markdownTexts
      .slice(1, 3)
      .every((text) => text.replace(/^>\s?/gm, '').replace(/\s+/g, ' ').includes(projectStatus)),
    'The getting-started guides do not carry the canonical project status.',
  );
}

async function crawlInternalLinks(page) {
  const routes = [
    '/',
    '/guides/first-pipeline/',
    '/guides/standalone-replicator/',
    '/guides/configure-postgres/',
    '/guides/custom-implementations/',
    '/explanation/concepts/',
    '/explanation/architecture/',
    '/explanation/events/',
    '/explanation/schema-changes/',
    '/reference/destinations/',
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
  await page.setViewportSize({ width: 2048, height: 1152 });
  await page.goto(`${baseUrl}/`, { waitUntil: 'networkidle' });
  await assertNoHorizontalOverflow(page);
  const wideHeroAlignment = await page.evaluate(() => {
    const title = document.querySelector('.etl-landing h1');
    const summary = document.querySelector('.etl-landing-summary');
    const headerBrand = document.querySelector('.etl-home-nav-brand');
    const headerLinks = document.querySelector('.etl-home-nav-links');
    const footerText = document.querySelector('.etl-home-footer p');
    const footerLinks = document.querySelector('.etl-home-footer nav');
    if (
      !(title instanceof HTMLElement) ||
      !(summary instanceof HTMLElement) ||
      !(headerBrand instanceof HTMLElement) ||
      !(headerLinks instanceof HTMLElement) ||
      !(footerText instanceof HTMLElement) ||
      !(footerLinks instanceof HTMLElement)
    ) return null;
    const titleRect = title.getBoundingClientRect();
    const summaryRect = summary.getBoundingClientRect();
    const headerBrandRect = headerBrand.getBoundingClientRect();
    const headerLinksRect = headerLinks.getBoundingClientRect();
    const footerTextRect = footerText.getBoundingClientRect();
    const footerLinksRect = footerLinks.getBoundingClientRect();
    const style = getComputedStyle(title);
    return {
      horizontalGap: summaryRect.left - titleRect.right,
      titleLines: Math.round(titleRect.height / Number.parseFloat(style.lineHeight)),
      shellEdgeDelta: Math.max(
        Math.abs(headerBrandRect.left - footerTextRect.left),
        Math.abs(headerLinksRect.right - footerLinksRect.right),
      ),
      contentEdgeDelta: Math.max(
        Math.abs(titleRect.left - headerBrandRect.left),
        Math.abs(summaryRect.right - headerLinksRect.right),
      ),
      bodyFont: getComputedStyle(document.body).fontFamily,
      headingFont: style.fontFamily,
      navFontSize: Number.parseFloat(
        getComputedStyle(headerLinks.querySelector('a')).fontSize,
      ),
    };
  });
  assert(
    wideHeroAlignment !== null &&
      wideHeroAlignment.horizontalGap >= 48 &&
      wideHeroAlignment.titleLines === 2 &&
      wideHeroAlignment.shellEdgeDelta <= 1 &&
      wideHeroAlignment.contentEdgeDelta <= 1 &&
      wideHeroAlignment.bodyFont.includes('Inter') &&
      wideHeroAlignment.headingFont.includes('Manrope') &&
      wideHeroAlignment.navFontSize >= 14,
    'The wide homepage headline overlaps or misaligns with the supporting copy.',
  );

  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.goto(`${baseUrl}/`, { waitUntil: 'networkidle' });
  await page.waitForTimeout(1500);
  await assertNoHorizontalOverflow(page);

  assert(
    (await page.locator('.etl-home-nav .etl-brand').innerText()) === 'Supabase ETL',
    'The homepage navigation does not use the full Supabase ETL name.',
  );
  assert(
    !(await page.locator('main').innerText()).includes('under active development'),
    'The homepage still exposes the active-development notice.',
  );
  assert(
    (await page.locator('#nd-docs-layout').count()) === 0,
    'The product homepage is still rendered inside the documentation shell.',
  );
  assert(
    (await page.locator('.etl-flow-conduit').count()) === 2 &&
      (await page.locator('.etl-data-packet').count()) === 18,
    'The homepage data-flow scene is missing its replication connections or packets.',
  );
  const flowGeometry = await page.evaluate(() => {
    const selectors = ['.etl-flow-node-source', '.etl-flow-node-core', '.etl-flow-node-destination'];
    const nodes = selectors.map((selector) => document.querySelector(selector));
    const headerItems = [
      document.querySelector('.etl-home-nav-brand'),
      document.querySelector('.etl-home-search'),
      document.querySelector('.etl-home-nav-links'),
    ];
    if (
      nodes.some((node) => !(node instanceof HTMLElement)) ||
      headerItems.some((item) => !(item instanceof HTMLElement))
    ) {
      return null;
    }
    const nodeRects = nodes.map((node) => node.getBoundingClientRect());
    const headerRects = headerItems.map((item) => item.getBoundingClientRect());
    const centers = nodeRects.map((rect) => rect.top + rect.height / 2);
    const headerCenters = headerRects.map((rect) => rect.top + rect.height / 2);
    return {
      ordered:
        nodeRects[0].right < nodeRects[1].left && nodeRects[1].right < nodeRects[2].left,
      centerDelta: Math.max(...centers) - Math.min(...centers),
      headerCenterDelta: Math.max(...headerCenters) - Math.min(...headerCenters),
    };
  });
  assert(
    flowGeometry?.ordered &&
      flowGeometry.centerDelta <= 2 &&
      flowGeometry.headerCenterDelta <= 2,
    `The homepage flow or header is not aligned: ${JSON.stringify(flowGeometry)}.`,
  );
  const flowSourceText = (await page.locator('.etl-flow-node-source').innerText())
    .replace(/\s+/g, ' ')
    .toLowerCase();
  const flowCoreText = (await page.locator('.etl-flow-node-core').innerText())
    .replace(/\s+/g, ' ')
    .toLowerCase();
  const flowDestinationLabels = ['analytics', 'search', 'cache', 'warehouse', 'data lake'];
  const flowDestinationText = (
    await page.locator('.etl-flow-node-destination').innerText()
  )
    .replace(/\s+/g, ' ')
    .toLowerCase();
  assert(
    flowSourceText.includes('source postgres') &&
      flowCoreText.includes('pipeline supabase etl') &&
      flowDestinationLabels.every((label) => flowDestinationText.includes(label)) &&
      (await page.locator('.etl-destination-face').count()) === flowDestinationLabels.length,
    'The data-flow scene does not explain source, ETL, and the arbitrary destinations it can cycle through.',
  );
  assert(
    (await page.locator('h1').innerText()).replace(/\s+/g, ' ') ===
      'Postgres replication, in Rust.',
    'Homepage heading changed unexpectedly.',
  );
  const heroTitleLines = await page.locator('h1').evaluate((title) => {
    const style = getComputedStyle(title);
    return Math.round(title.getBoundingClientRect().height / Number.parseFloat(style.lineHeight));
  });
  assert(heroTitleLines === 2, 'Homepage heading does not use the intended two-line lockup.');
  assert(
    (await page.locator('link[rel="canonical"]').getAttribute('href')) ===
      `${canonicalBaseUrl}/`,
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
      `${canonicalBaseUrl}/index.md`,
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

  await page.getByRole('button', { name: 'Search Supabase ETL documentation' }).click();
  const searchInput = page.getByRole('textbox', { name: 'Search Supabase ETL documentation' });
  await searchInput.fill('replica identity');
  const searchInputFocusStyle = await searchInput.evaluate((input) => {
    const style = getComputedStyle(input);
    return {
      active: document.activeElement === input,
      outlineStyle: style.outlineStyle,
      boxShadow: style.boxShadow,
    };
  });
  assert(
    searchInputFocusStyle.active &&
      searchInputFocusStyle.outlineStyle === 'none' &&
      searchInputFocusStyle.boxShadow === 'none',
    `The focused search input has an unwanted selection rectangle: ${JSON.stringify(searchInputFocusStyle)}.`,
  );
  await page.getByRole('button', { name: /Logical Replication/ }).first().waitFor();
  const searchPanelMetrics = await page.locator('#fd-search-dialog-content').evaluate((panel) => {
    const rect = panel.getBoundingClientRect();
    const style = getComputedStyle(panel);
    return {
      width: rect.width,
      top: rect.top,
      borderRadius: Number.parseFloat(style.borderRadius),
      horizontalMargin: Math.min(rect.left, window.innerWidth - rect.right),
    };
  });
  assert(
    searchPanelMetrics.width <= 562 &&
      searchPanelMetrics.top >= 64 &&
      searchPanelMetrics.borderRadius >= 15 &&
      searchPanelMetrics.horizontalMargin >= 16,
    'The search panel sizing, margin, or border radius is not aligned with the homepage.',
  );
  await searchInput.press('Escape');

  const homeChatGptHref = await page.getByRole('link', { name: 'Ask ChatGPT' }).getAttribute('href');
  const homeClaudeHref = await page.getByRole('link', { name: 'Ask Claude' }).getAttribute('href');
  const homeAgentPrompt = `Read ${baseUrl}/, I want to ask questions about it.`;
  assert(
    new URL(homeChatGptHref).searchParams.get('q') === homeAgentPrompt &&
      new URL(homeClaudeHref).searchParams.get('q') === homeAgentPrompt,
    'Homepage agent actions do not use the current deployment URL.',
  );
  const homeCopyResponsePromise = page.waitForResponse(
    (response) => response.url() === `${baseUrl}/index.md`,
  );
  await page.getByRole('button', { name: 'Copy Markdown' }).click();
  const homeCopyResponse = await homeCopyResponsePromise;
  assert(
    homeCopyResponse.ok() && homeCopyResponse.headers()['content-type']?.includes('text/markdown'),
    'Homepage Copy Markdown does not fetch the generated Markdown page.',
  );
  await page.getByRole('button', { name: 'Copied' }).waitFor();
  const homeClipboardText = await page.evaluate(() => navigator.clipboard.readText());
  assert(
    homeClipboardText.startsWith('# Home') &&
      homeClipboardText.includes('## Documentation map') &&
      !/<(?:!doctype|html|body)\b/i.test(homeClipboardText),
    'Homepage Copy Markdown copied HTML instead of the generated Markdown.',
  );

  await page.goto(`${baseUrl}/guides/first-pipeline/`, { waitUntil: 'networkidle' });
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
    'Get started',
    'First Pipeline',
    'Standalone Replicator',
    'Guides',
    'Configure Postgres',
    'Custom Implementations',
    'Concepts',
    'Logical Replication',
    'Architecture',
    'Schema Changes',
    'Reference',
    'Destinations',
    'Events',
    'Extension Points',
  ]) {
    assert(sidebarText.includes(label), `Sidebar is missing the concise label: ${label}.`);
  }
  assert(!sidebarText.includes('Home'), 'The docs sidebar should begin with First Pipeline.');
  const standaloneSidebarLink = page.locator(
    '#nd-sidebar a[href="/etl/guides/standalone-replicator/"]',
  );
  assert(
    (await standaloneSidebarLink.count()) === 1 &&
      (await standaloneSidebarLink.locator('svg').count()) === 1,
    'Standalone Replicator is missing its sidebar icon.',
  );
  const destinationsSidebarLink = page.locator(
    '#nd-sidebar a[href="/etl/reference/destinations/"]',
  );
  assert(
    (await destinationsSidebarLink.count()) === 1 &&
      (await destinationsSidebarLink.locator('svg').count()) === 1,
    'Destinations is missing its sidebar icon.',
  );
  for (const oldLabel of [
    'Build Your First ETL Pipeline',
    'Configure Postgres for Replication',
    'Postgres Logical Replication Concepts',
    'Supabase ETL Architecture',
    'Event Types',
  ]) {
    assert(!sidebarText.includes(oldLabel), `Sidebar still contains the long label: ${oldLabel}.`);
  }
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
  const guideChatGptHref = await page.getByRole('link', { name: 'Ask ChatGPT' }).getAttribute('href');
  const guideClaudeHref = await page.getByRole('link', { name: 'Ask Claude' }).getAttribute('href');
  const guideAgentPrompt =
    `Read ${baseUrl}/guides/first-pipeline/, I want to ask questions about it.`;
  assert(
    new URL(guideChatGptHref).searchParams.get('q') === guideAgentPrompt &&
      new URL(guideClaudeHref).searchParams.get('q') === guideAgentPrompt,
    'Guide agent actions do not use the current deployment URL.',
  );
  assert(
    (await page.locator('link[rel="canonical"]').getAttribute('href')) ===
      `${canonicalBaseUrl}/guides/first-pipeline/`,
    'Guide metadata does not use the canonical trailing-slash URL.',
  );
  const guideStructuredData = JSON.parse(
    await page.locator('script[type="application/ld+json"]').textContent(),
  );
  assert(
    guideStructuredData.url ===
      `${canonicalBaseUrl}/guides/first-pipeline/` &&
      guideStructuredData.mainEntityOfPage ===
        `${canonicalBaseUrl}/guides/first-pipeline/`,
    'Guide structured data does not use the canonical trailing-slash URL.',
  );

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
  await page.getByRole('button', { name: 'Copied' }).waitFor();
  const guideClipboardText = await page.evaluate(() => navigator.clipboard.readText());
  assert(
    guideClipboardText.startsWith('# First Pipeline') &&
      guideClipboardText.includes('## Create the project') &&
      !/<(?:!doctype|html|body)\b/i.test(guideClipboardText),
    'Guide Copy Markdown copied HTML instead of the generated Markdown.',
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

  await page.goto(`${baseUrl}/reference/destinations/`, { waitUntil: 'networkidle' });
  await assertNoHorizontalOverflow(page);
  assert(
    (await page.locator('.etl-destination-status[data-status="stable"] svg').count()) === 1 &&
      (await page.locator('.etl-destination-status[data-status="in-progress"] svg').count()) === 3 &&
      (await page.locator('.etl-destination-status[data-status="deprecated"] svg').count()) === 1,
    'Destination maturity badges are missing their status icons.',
  );
  assert(
    (await page.locator('#nd-page').innerText()).includes('BigQuery is the stable, recommended default.'),
    'The Destinations reference does not identify BigQuery as the default.',
  );
  await page.route(`${baseUrl}/reference/destinations.md`, (route) =>
    route.fulfill({ status: 404, contentType: 'text/html', body: '<!doctype html><title>Missing</title>' }),
  );
  const generatedMarkdownResponse = page.waitForResponse(
    (response) =>
      response.url() === `${baseUrl}/llms.mdx/reference/destinations/content.md` && response.ok(),
  );
  await page.getByRole('button', { name: 'Copy Markdown' }).click();
  await generatedMarkdownResponse;
  await page.getByRole('button', { name: 'Copied' }).waitFor();
  const fallbackClipboardText = await page.evaluate(() => navigator.clipboard.readText());
  assert(
    fallbackClipboardText.startsWith('# Destinations') &&
      !/<(?:!doctype|html|body)\b/i.test(fallbackClipboardText),
    'Copy Markdown did not use the generated Markdown fallback after an alias failure.',
  );
  await page.unroute(`${baseUrl}/reference/destinations.md`);

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
  await page.waitForTimeout(1500);
  await assertNoHorizontalOverflow(page);
  assert(
    (await page.locator('.etl-flow-node-source').count()) === 1 &&
      (await page.locator('.etl-flow-node-core').count()) === 1 &&
      (await page.locator('.etl-flow-node-destination').count()) === 1,
    'The mobile homepage is missing the replication scene.',
  );
  const mobileFlowGeometry = await page
    .locator('.etl-flow-stage')
    .evaluate((stage) => {
      const nodes = [
        stage.querySelector('.etl-flow-node-source'),
        stage.querySelector('.etl-flow-node-core'),
        stage.querySelector('.etl-flow-node-destination'),
      ];
      if (nodes.some((node) => !(node instanceof HTMLElement))) return null;
      const rects = nodes.map((node) => node.getBoundingClientRect());
      const centers = rects.map((rect) => rect.top + rect.height / 2);
      return {
        ordered: rects[0].right < rects[1].left && rects[1].right < rects[2].left,
        centerDelta: Math.max(...centers) - Math.min(...centers),
      };
    });
  assert(
    mobileFlowGeometry?.ordered && mobileFlowGeometry.centerDelta <= 2,
    `The mobile replication flow is not aligned: ${JSON.stringify(mobileFlowGeometry)}.`,
  );
  const mobileHeroFit = await page.locator('.etl-landing-content').evaluate((content) => {
    const rect = content.getBoundingClientRect();
    const actions = [...content.querySelectorAll('a, button')];
    return (
      rect.left >= 0 &&
      rect.right <= window.innerWidth &&
      actions.every((action) => {
        const actionRect = action.getBoundingClientRect();
        return actionRect.left >= 0 && actionRect.right <= window.innerWidth;
      })
    );
  });
  assert(mobileHeroFit, 'A mobile homepage action is clipped or overflows the viewport.');
  assert(
    (await page.locator('.etl-home-search-compact').isVisible()) &&
      !(await page.locator('.etl-home-search-full').isVisible()),
    'The homepage search control does not adapt to mobile.',
  );
  assert(
    (await page.locator('.etl-landing h1').innerText()).replace(/\s+/g, ' ') ===
      'Postgres replication, in Rust.',
    'The mobile homepage changed the product statement.',
  );

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
    const sidebar = document.querySelector('#nd-sidebar-mobile');
    const activeLink = sidebar?.querySelector('a[data-active="true"]');
    const sidebarRect = sidebar?.getBoundingClientRect();
    const activeLinkRect = activeLink?.getBoundingClientRect();

    return {
      footerBorderTop: footer ? getComputedStyle(footer).borderTopWidth : null,
      fitsViewport: rect.left >= 0 && rect.right <= window.innerWidth,
      sidebarRect: sidebarRect
        ? { left: sidebarRect.left, right: sidebarRect.right, width: sidebarRect.width }
        : null,
      activeLinkRect: activeLinkRect
        ? { left: activeLinkRect.left, right: activeLinkRect.right, width: activeLinkRect.width }
        : null,
      contentAligned:
        sidebarRect && activeLinkRect
          ? activeLinkRect.left - sidebarRect.left <= 24 &&
            sidebarRect.right - activeLinkRect.right <= 24 &&
            rect.left - sidebarRect.left <= 24 &&
            sidebarRect.right - rect.right <= 24
          : false,
    };
  });
  assert(
    mobilePipelinesStyle.footerBorderTop === '0px' &&
      mobilePipelinesStyle.fitsViewport &&
      mobilePipelinesStyle.contentAligned,
    `The mobile sidebar alignment, Pipelines divider, or sizing is incorrect: ${JSON.stringify(mobilePipelinesStyle)}.`,
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

  await page.emulateMedia({ reducedMotion: 'reduce' });
  await page.goto(`${baseUrl}/`, { waitUntil: 'networkidle' });
  const reducedMotion = await page.locator('.etl-data-packet').first().evaluate((packet) => {
    const style = getComputedStyle(packet);
    const duration = Number.parseFloat(style.animationDuration);
    return {
      durationMs: style.animationDuration.endsWith('ms') ? duration : duration * 1000,
      animationName: style.animationName,
      opacity: Number.parseFloat(style.opacity),
    };
  });
  assert(
    reducedMotion.durationMs <= 0.01 &&
      reducedMotion.animationName === 'none' &&
      reducedMotion.opacity >= 0.8,
    'The replication scene does not provide a reduced-motion presentation.',
  );
  await page.emulateMedia({ reducedMotion: 'no-preference' });
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
  await checkSourceCodeFences();
  await checkProjectStatusConsistency();
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
    'Docs smoke checks passed: code languages, landing motion, SEO, agent feeds, page actions, search, diagrams, responsive UI, and links.',
  );
} finally {
  if (browser) await Promise.race([browser.close(), delay(cleanupTimeoutMs)]);
  await stopPreview();
}
