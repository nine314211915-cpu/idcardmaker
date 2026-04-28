(function () {
  let deferredInstallPrompt = null;

  function isStandalone() {
    return window.matchMedia('(display-mode: standalone)').matches || window.navigator.standalone;
  }

  function createInstallButton() {
    if (document.getElementById('pwaInstallButton') || isStandalone()) return null;
    const button = document.createElement('button');
    button.id = 'pwaInstallButton';
    button.type = 'button';
    button.textContent = 'Install App';
    button.setAttribute('aria-label', 'Install ID Card System app');
    button.style.cssText = [
      'position:fixed',
      'right:14px',
      'bottom:14px',
      'z-index:9999',
      'border:0',
      'border-radius:14px',
      'padding:11px 14px',
      'background:#0f4c81',
      'color:#fff',
      'font:700 13px Public Sans,Arial,sans-serif',
      'box-shadow:0 14px 34px rgba(15,76,129,.28)',
      'cursor:pointer'
    ].join(';');
    document.body.appendChild(button);
    return button;
  }

  function showIosHint() {
    if (document.getElementById('pwaInstallHint')) return;
    const hint = document.createElement('div');
    hint.id = 'pwaInstallHint';
    hint.textContent = 'Use browser menu and Add to Home screen.';
    hint.style.cssText = [
      'position:fixed',
      'left:12px',
      'right:12px',
      'bottom:68px',
      'z-index:9999',
      'padding:10px 12px',
      'border-radius:12px',
      'background:#ffffff',
      'color:#212529',
      'font:600 12px Public Sans,Arial,sans-serif',
      'box-shadow:0 14px 34px rgba(15,23,42,.18)',
      'text-align:center'
    ].join(';');
    document.body.appendChild(hint);
    setTimeout(() => hint.remove(), 5200);
  }

  async function registerServiceWorker() {
    if (!('serviceWorker' in navigator)) return;
    try {
      await navigator.serviceWorker.register('/service-worker.js');
    } catch (error) {}
  }

  window.addEventListener('beforeinstallprompt', (event) => {
    event.preventDefault();
    deferredInstallPrompt = event;
    const button = createInstallButton();
    if (!button) return;
    button.onclick = async () => {
      button.style.display = 'none';
      deferredInstallPrompt.prompt();
      await deferredInstallPrompt.userChoice;
      deferredInstallPrompt = null;
    };
  });

  window.addEventListener('appinstalled', () => {
    const button = document.getElementById('pwaInstallButton');
    if (button) button.remove();
  });

  document.addEventListener('DOMContentLoaded', () => {
    registerServiceWorker();
    const isiOS = /iphone|ipad|ipod/i.test(navigator.userAgent);
    if (isiOS && !isStandalone()) {
      const button = createInstallButton();
      if (button) button.onclick = showIosHint;
    }
  });
})();
