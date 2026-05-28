(function () {
  const mobileQuery = window.matchMedia('(max-width: 50rem)');

  function sidebarMenuButton() {
    return document.querySelector('button[aria-controls="starlight__sidebar"]');
  }

  function sidebarMenuHost() {
    return document.querySelector('starlight-menu-button');
  }

  function sidebarIsOpen() {
    const host = sidebarMenuHost();
    const button = sidebarMenuButton();
    return host?.getAttribute('aria-expanded') === 'true'
      || button?.getAttribute('aria-expanded') === 'true';
  }

  function closeSidebar() {
    const button = sidebarMenuButton();
    if (mobileQuery.matches && sidebarIsOpen() && button) {
      button.click();
    }
  }

  function closeMobileToc() {
    document.querySelector('#starlight__mobile-toc[open]')?.removeAttribute('open');
  }

  function installMobileNavBehavior() {
    const sidebar = document.querySelector('#starlight__sidebar');
    if (sidebar && !sidebar.dataset.mobileNavReady) {
      sidebar.dataset.mobileNavReady = 'true';
      sidebar.addEventListener('click', (event) => {
        if (event.target instanceof Element && event.target.closest('a[href]')) {
          closeSidebar();
        }
      });
    }

    const mobileToc = document.querySelector('mobile-starlight-toc');
    if (mobileToc && !mobileToc.dataset.mobileNavReady) {
      mobileToc.dataset.mobileNavReady = 'true';
      mobileToc.addEventListener('click', (event) => {
        if (event.target instanceof Element && event.target.closest('a[href]')) {
          closeMobileToc();
        }
      });
    }
  }

  document.addEventListener('click', (event) => {
    if (!mobileQuery.matches || !(event.target instanceof Element)) {
      return;
    }

    const target = event.target;

    if (
      sidebarIsOpen()
      && !target.closest('#starlight__sidebar')
      && !target.closest('starlight-menu-button')
    ) {
      closeSidebar();
    }

    if (
      document.querySelector('#starlight__mobile-toc[open]')
      && !target.closest('mobile-starlight-toc')
    ) {
      closeMobileToc();
    }
  });

  document.addEventListener('keydown', (event) => {
    if (event.key !== 'Escape') {
      return;
    }

    closeSidebar();
    closeMobileToc();
  });

  document.addEventListener('DOMContentLoaded', installMobileNavBehavior);
  document.addEventListener('astro:page-load', installMobileNavBehavior);
})();
