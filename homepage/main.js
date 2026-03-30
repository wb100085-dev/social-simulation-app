/* ===========================
   OmniNode Main JS
=========================== */

// ── NAV SCROLL ──────────────────────────────
const nav = document.getElementById('nav');
if (nav) {
  window.addEventListener('scroll', () => {
    nav.classList.toggle('scrolled', window.scrollY > 40);
  });
}

// ── MOBILE MENU ─────────────────────────────
const hamburger = document.getElementById('hamburger');
const mobileMenu = document.getElementById('mobileMenu');

if (hamburger && mobileMenu) {
  hamburger.addEventListener('click', () => {
    const open = mobileMenu.style.display === 'block';
    mobileMenu.style.display = open ? 'none' : 'block';
  });

  mobileMenu.querySelectorAll('a').forEach(link => {
    link.addEventListener('click', () => {
      mobileMenu.style.display = 'none';
    });
  });
}

// ── NETWORK CANVAS (메인 히어로에만 존재) ───
const canvas = document.getElementById('networkCanvas');
const ctx = canvas ? canvas.getContext('2d') : null;

let nodes = [];
let animFrameId;

function resizeCanvas() {
  if (!canvas) return;
  canvas.width  = canvas.offsetWidth;
  canvas.height = canvas.offsetHeight;
}

function randomBetween(min, max) {
  return Math.random() * (max - min) + min;
}

function initNodes() {
  if (!canvas) return;
  const count = Math.floor((canvas.width * canvas.height) / 18000);
  nodes = [];
  for (let i = 0; i < count; i++) {
    nodes.push({
      x: randomBetween(0, canvas.width),
      y: randomBetween(0, canvas.height),
      vx: randomBetween(-0.3, 0.3),
      vy: randomBetween(-0.3, 0.3),
      r: randomBetween(1.5, 3),
    });
  }
}

function drawNetwork() {
  if (!canvas || !ctx) return;
  ctx.clearRect(0, 0, canvas.width, canvas.height);

  // Draw connections
  for (let i = 0; i < nodes.length; i++) {
    for (let j = i + 1; j < nodes.length; j++) {
      const dx = nodes[i].x - nodes[j].x;
      const dy = nodes[i].y - nodes[j].y;
      const dist = Math.sqrt(dx * dx + dy * dy);
      const maxDist = 160;

      if (dist < maxDist) {
        const alpha = (1 - dist / maxDist) * 0.35;
        const grad = ctx.createLinearGradient(nodes[i].x, nodes[i].y, nodes[j].x, nodes[j].y);
        grad.addColorStop(0, `rgba(232,105,42,${alpha})`);
        grad.addColorStop(1, `rgba(240,160,96,${alpha})`);
        ctx.beginPath();
        ctx.moveTo(nodes[i].x, nodes[i].y);
        ctx.lineTo(nodes[j].x, nodes[j].y);
        ctx.strokeStyle = grad;
        ctx.lineWidth = 0.8;
        ctx.stroke();
      }
    }
  }

  // Draw nodes
  nodes.forEach(n => {
    const grad = ctx.createRadialGradient(n.x, n.y, 0, n.x, n.y, n.r * 2.5);
    grad.addColorStop(0, 'rgba(232,105,42,0.8)');
    grad.addColorStop(1, 'rgba(240,160,96,0)');
    ctx.beginPath();
    ctx.arc(n.x, n.y, n.r, 0, Math.PI * 2);
    ctx.fillStyle = grad;
    ctx.fill();

    // core dot
    ctx.beginPath();
    ctx.arc(n.x, n.y, n.r * 0.6, 0, Math.PI * 2);
    ctx.fillStyle = 'rgba(253,186,116,0.9)';
    ctx.fill();
  });
}

function updateNodes() {
  if (!canvas) return;
  nodes.forEach(n => {
    n.x += n.vx;
    n.y += n.vy;
    if (n.x < 0 || n.x > canvas.width)  n.vx *= -1;
    if (n.y < 0 || n.y > canvas.height) n.vy *= -1;
  });
}

function loop() {
  updateNodes();
  drawNetwork();
  animFrameId = requestAnimationFrame(loop);
}

if (canvas && ctx) {
  window.addEventListener('resize', () => {
    resizeCanvas();
    initNodes();
  });

  resizeCanvas();
  initNodes();
  loop();
}

// ── COUNTER ANIMATION ───────────────────────
function animateCounter(el) {
  const target = parseInt(el.dataset.target, 10);
  const duration = 2000;
  const start = performance.now();

  function step(now) {
    const elapsed = now - start;
    const progress = Math.min(elapsed / duration, 1);
    const eased = 1 - Math.pow(1 - progress, 3); // ease-out cubic
    el.textContent = Math.floor(eased * target);
    if (progress < 1) requestAnimationFrame(step);
    else el.textContent = target;
  }
  requestAnimationFrame(step);
}

// ── SCROLL REVEAL ───────────────────────────
const revealEls = [];

function addReveal(selector, delayIndex = false) {
  document.querySelectorAll(selector).forEach((el, i) => {
    el.classList.add('reveal');
    if (delayIndex && i < 4) el.classList.add(`reveal-delay-${i + 1}`);
    revealEls.push(el);
  });
}

addReveal('.pain-card', true);
addReveal('.product__feature', true);
addReveal('.product__screen');
addReveal('.service-card', true);
addReveal('.tech-card', true);
addReveal('.process__step', true);
addReveal('.market__step', true);
addReveal('.value-item', true);
addReveal('.section-header');
addReveal('.about__text');
addReveal('.about__visual');
addReveal('.contact__info');
addReveal('.contact__form');

let countersStarted = false;

const observer = new IntersectionObserver((entries) => {
  entries.forEach(entry => {
    if (entry.isIntersecting) {
      entry.target.classList.add('visible');
    }
  });
}, { threshold: 0.12 });

revealEls.forEach(el => observer.observe(el));

// Counter observer
const heroStats = document.querySelector('.hero__stats');
const counterObserver = new IntersectionObserver((entries) => {
  entries.forEach(entry => {
    if (entry.isIntersecting && !countersStarted) {
      countersStarted = true;
      document.querySelectorAll('.stat__num[data-target]').forEach(animateCounter);
    }
  });
}, { threshold: 0.5 });

if (heroStats) counterObserver.observe(heroStats);

// ── SMOOTH SCROLL ───────────────────────────
document.querySelectorAll('a[href^="#"]').forEach(a => {
  a.addEventListener('click', e => {
    const target = document.querySelector(a.getAttribute('href'));
    if (target) {
      e.preventDefault();
      target.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }
  });
});

// ── CONTACT FORM (Formspree) ─────────────────
const contactForm = document.getElementById('contactForm');
const toast = document.getElementById('toast');

if (contactForm) {
  contactForm.addEventListener('submit', async (e) => {
    e.preventDefault();
    const action = contactForm.getAttribute('action') || '';
    if (action.includes('REPLACE_WITH_YOUR_ID')) {
      alert('문의 전송을 켜려면 Formspree에서 폼을 만든 뒤, index.html의 form action을 발급받은 URL(…/f/xxxx)으로 바꿔주세요.');
      return;
    }

    const btn = contactForm.querySelector('button[type="submit"]');
    const prevText = btn.textContent;
    btn.textContent = '전송 중...';
    btn.disabled = true;

    try {
      const res = await fetch(action, {
        method: 'POST',
        body: new FormData(contactForm),
        headers: { Accept: 'application/json' },
      });
      let data = {};
      try {
        data = await res.json();
      } catch (_) {}

      if (res.ok) {
        contactForm.reset();
        showToast();
      } else {
        const msg = (data && data.error) || '전송에 실패했습니다. 잠시 후 다시 시도해 주세요.';
        alert(msg);
      }
    } catch (_) {
      alert('네트워크 오류로 전송하지 못했습니다.');
    } finally {
      btn.textContent = prevText;
      btn.disabled = false;
    }
  });
}

function showToast() {
  if (!toast) return;
  toast.classList.add('show');
  setTimeout(() => toast.classList.remove('show'), 3500);
}

// ── ACTIVE NAV LINK ─────────────────────────
const sections = document.querySelectorAll('section[id]');
const navLinks = document.querySelectorAll('.nav__links a');

if (sections.length && navLinks.length) {
  window.addEventListener('scroll', () => {
    let current = '';
    sections.forEach(sec => {
      if (window.scrollY >= sec.offsetTop - 120) {
        current = sec.id;
      }
    });
    navLinks.forEach(link => {
      link.style.color = '';
      const href = link.getAttribute('href') || '';
      if (href === `#${current}` || href.endsWith(`#${current}`)) {
        link.style.color = 'var(--text-primary)';
      }
    });
  }, { passive: true });
}

// ── CURSOR GLOW (desktop only) ───────────────
if (window.innerWidth > 768) {
  const glow = document.createElement('div');
  glow.style.cssText = `
    position:fixed;pointer-events:none;z-index:9999;
    width:400px;height:400px;border-radius:50%;
    background:radial-gradient(circle,rgba(232,105,42,0.06) 0%,transparent 70%);
    transform:translate(-50%,-50%);
    transition:left 0.8s ease,top 0.8s ease;
    left:-200px;top:-200px;
  `;
  document.body.appendChild(glow);

  window.addEventListener('mousemove', e => {
    glow.style.left = e.clientX + 'px';
    glow.style.top  = e.clientY + 'px';
  }, { passive: true });
}
