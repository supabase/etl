(function () {
  const lightThemeVariables = {
    background: '#ffffff',
    primaryColor: '#ffffff',
    primaryTextColor: '#262626',
    primaryBorderColor: '#d4d4d4',
    lineColor: '#a3a3a3',
    secondaryColor: '#f7f7f7',
    tertiaryColor: '#f3f4f6',
    clusterBkg: '#fafafa',
    clusterBorder: '#e5e5e5',
    edgeLabelBackground: '#ffffff',
    fontFamily: 'Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif',
  };

  const darkThemeVariables = {
    background: '#080d0b',
    primaryColor: '#10231b',
    primaryTextColor: '#e6f3ed',
    primaryBorderColor: '#3ecf8e',
    lineColor: '#55dba0',
    secondaryColor: '#0f1a16',
    tertiaryColor: '#15241d',
    clusterBkg: '#0b120f',
    clusterBorder: '#275240',
    edgeLabelBackground: '#0b120f',
    fontFamily: 'Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif',
  };

  let renderQueued = false;
  let lastRenderedTheme = '';

  function currentTheme() {
    return document.documentElement.dataset.theme === 'dark' ? 'dark' : 'light';
  }

  function mermaidThemeVariables() {
    return currentTheme() === 'dark' ? darkThemeVariables : lightThemeVariables;
  }

  function initializeMermaid() {
    if (!window.mermaid) {
      return;
    }

    window.mermaid.initialize({
      startOnLoad: false,
      securityLevel: 'strict',
      theme: 'base',
      themeVariables: mermaidThemeVariables(),
    });
  }

  function waitForMermaid() {
    if (window.mermaid) {
      return Promise.resolve(true);
    }

    if (typeof window.loadMermaid === 'function') {
      return window.loadMermaid().then(Boolean).catch(() => false);
    }

    return new Promise((resolve) => {
      const timeout = window.setTimeout(() => resolve(false), 3000);
      window.addEventListener(
        'mermaid:ready',
        () => {
          window.clearTimeout(timeout);
          resolve(true);
        },
        { once: true },
      );
    });
  }

  function collectMermaidBlocks() {
    document.querySelectorAll('pre[data-language="mermaid"], pre code.language-mermaid').forEach((block) => {
      const pre = block.matches('pre') ? block : block.parentElement;
      if (!pre || pre.dataset.mermaidRendered === 'true') {
        return;
      }

      const container = document.createElement('div');
      container.className = 'mermaid';
      container.dataset.mermaidSource = pre.innerText;
      container.textContent = container.dataset.mermaidSource;
      const expressiveCode = pre.closest('.expressive-code');
      if (expressiveCode) {
        expressiveCode.replaceWith(container);
      } else {
        pre.replaceWith(container);
      }
      pre.dataset.mermaidRendered = 'true';
    });
  }

  function resetRenderedMermaidBlocks() {
    document.querySelectorAll('.mermaid[data-mermaid-source]').forEach((container) => {
      container.removeAttribute('data-processed');
      container.textContent = container.dataset.mermaidSource;
    });
  }

  async function renderMermaidBlocks({ force = false } = {}) {
    if (!(await waitForMermaid())) {
      return;
    }

    collectMermaidBlocks();

    const theme = currentTheme();
    if (force || theme !== lastRenderedTheme) {
      resetRenderedMermaidBlocks();
    }

    initializeMermaid();
    lastRenderedTheme = theme;

    await window.mermaid.run({ querySelector: '.mermaid' });
  }

  function queueRender(force = false) {
    if (renderQueued) {
      return;
    }

    renderQueued = true;
    window.requestAnimationFrame(() => {
      renderQueued = false;
      renderMermaidBlocks({ force });
    });
  }

  const themeObserver = new MutationObserver((mutations) => {
    if (mutations.some((mutation) => mutation.attributeName === 'data-theme')) {
      queueRender(true);
    }
  });

  themeObserver.observe(document.documentElement, {
    attributes: true,
    attributeFilter: ['data-theme'],
  });

  document.addEventListener('DOMContentLoaded', () => queueRender());
  document.addEventListener('astro:page-load', () => queueRender());
})();
