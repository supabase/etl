(function () {
  const languageLabels = {
    bash: 'Shell',
    console: 'Shell',
    dockerfile: 'Dockerfile',
    ini: 'INI',
    json: 'JSON',
    mermaid: '',
    rust: 'Rust',
    sh: 'Shell',
    shell: 'Shell',
    sql: 'SQL',
    text: 'Text',
    toml: 'TOML',
    yaml: 'YAML',
    yml: 'YAML',
  };

  function titleCaseLanguage(language) {
    return language
      .split(/[-_]/)
      .filter(Boolean)
      .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
      .join(' ');
  }

  function labelCodeBlocks() {
    document.querySelectorAll('pre[data-language]').forEach((pre) => {
      const figure = pre.closest('figure');
      const language = pre.dataset.language || '';
      const normalizedLanguage = language.trim().toLowerCase();

      if (!normalizedLanguage) {
        return;
      }

      const label = Object.hasOwn(languageLabels, normalizedLanguage)
        ? languageLabels[normalizedLanguage]
        : titleCaseLanguage(normalizedLanguage);

      if (!label) {
        return;
      }

      const labelTarget = figure || pre;
      labelTarget.dataset.language = normalizedLanguage;
      labelTarget.dataset.languageLabel = label;
    });
  }

  document.addEventListener('DOMContentLoaded', labelCodeBlocks);
  document.addEventListener('astro:page-load', labelCodeBlocks);
})();
