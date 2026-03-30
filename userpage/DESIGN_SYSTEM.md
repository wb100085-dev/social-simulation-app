# SocialTwin / ㈜옴니노드 — 디자인 시스템 가이드

다른 페이지·사이트에 **메인 페이지와 동일한 룩앤필**을 적용할 때 참고하는 문서입니다. 실제 구현은 `styles.css`가 단일 소스입니다. 이 파일은 **변수·패턴·HTML 골격**을 요약합니다.

---

## 1. 전체 인상

- **테마:** 다크 네이비 베이스 + 오렌지/피치 액센트(SocialTwin 브랜드).
- **톤:** B2B/SaaS 랜딩 — 여백 넉넉, 카드형 섹션, 그라데이션 포인트.
- **기술 스택:** 순수 HTML/CSS/JS. 레이아웃은 Flexbox + CSS Grid.

---

## 2. 필수 연결 (새 HTML 페이지 최소 설정)

`index.html`과 동일하게 `<head>`에 다음을 넣습니다.

```html
<link rel="stylesheet" href="styles.css?v=2" />
<link rel="preconnect" href="https://fonts.googleapis.com" />
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&family=Noto+Sans+KR:wght@300;400;500;600;700;800;900&display=swap" rel="stylesheet" />
```

- **본문:** `body`는 별도 클래스 없이도 기본 다크 배경·본문 색이 적용됩니다.
- **고정 네비 아래 첫 섹션:** `index`와 같이 히어로가 없다면, 첫 섹션에 상단 패딩을 충분히 주거나 `socialtwin-detail.html`처럼 `body.page-socialtwin-detail .technology { padding-top: 120px; }` 패턴을 복제합니다.

---

## 3. CSS 변수 (`:root`) — 색·반경·타이포

새 프로젝트로 옮길 때 **`styles.css` 맨 위 `:root` 블록을 그대로 복사**하면 팔레트가 동일해집니다.

| 토큰 | 용도 |
|-----|------|
| `--bg-dark` `#192848` | 페이지 배경 |
| `--bg-card` `#1d3060` | 카드·폼 배경 |
| `--bg-card-2` `#162444` | 보조 카드/딥 배경 |
| `--border` | 오렌지 톤 테두리(강조) |
| `--border-soft` | 은은한 구분선 |
| `--indigo` `#e8692a` | 주 액센트(링크·아이콘·포인트) |
| `--cyan` `#f0a060` | 보조 액센트·태그 |
| `--text-primary` `#f1f5f9` | 본문 밝은 글자 |
| `--text-secondary` `#94a3b8` | 부연 설명 |
| `--text-muted` `#475569` | 덜 강조 |
| `--gradient-main` | 버튼·그라데이션 텍스트 |
| `--gradient-glow` | 카드 호버 글로우 |
| `--radius-sm` ~ `--radius-xl` | 8 ~ 32px |
| `--font-body` | `'Noto Sans KR', 'Inter', sans-serif` |
| `--transition` | `0.3s cubic-bezier(0.4, 0, 0.2, 1)` |

**그라데이션 텍스트 클래스:** `.gradient-text` — 제목 강조에 사용.

---

## 4. 레이아웃

| 클래스 | 설명 |
|--------|------|
| `.container` | `max-width: 1200px`, 좌우 `24px` 패딩. 섹션 내 콘텐츠는 이 안에 둡니다. |

섹션별로 `padding: 100px 0` 전후 여백이 많습니다. 새 섹션을 추가할 때 기존 `.services`, `.market` 등과 동일한 리듬을 맞추려면 **세로 80~100px 대**를 참고합니다.

---

## 5. 타이포그래피

- **본문:** `line-height: 1.7`, 색 `var(--text-primary)` 또는 설명은 `var(--text-secondary)`.
- **섹션 머리말 패턴 (가운데 정렬 블록):**
  - `.section-eyebrow` — 소제목, 대문자 느낌, 자간 넓음, 그라데이션 텍스트.
  - `.section-title` — `clamp(2rem, 4vw, 3rem)`, 굵기 800.
  - `.section-desc` — 한 단락 설명, `var(--text-secondary)`.

```html
<div class="section-header">
  <p class="section-eyebrow">LABEL</p>
  <h2 class="section-title">큰 제목</h2>
  <p class="section-desc">설명 문단…</p>
</div>
```

---

## 6. 버튼

| 클래스 | 용도 |
|--------|------|
| `.btn` | 기본 (필수 조합) |
| `.btn--primary` | 주요 CTA — 오렌지 그라데이션, 알약형 (`border-radius: 50px`) |
| `.btn--ghost` | 보조 — 테두리만, 호버 시 은은한 오렌지 배경 |
| `.btn--white` | 어두운 배경 위 히어로/배너용 흰 버튼 |
| `.btn--full` | 폼 제출 등 전폭 |

애니메이션: 호버 시 `translateY(-2px)` + 그림자 강화.

---

## 7. 네비게이션 패턴

클래스 접두사 `.nav`, `.nav__inner`, `.nav__logo`, `.nav__links`, `.nav__cta`, `.nav__hamburger`, `.nav__mobile`.

- **스크롤 시:** JS가 `#nav`에 `.scrolled`를 붙이면 반투명 배경 + blur.
- **로고:** `.logo-product` / `.logo-company` 이중 텍스트 + `.logo-img` 아이콘.
- 다른 페이지에서도 동일 마크업을 쓰면 스타일이 일치합니다. `main.js`의 스크롤·햄버거는 선택 사항(캔버스 없는 페이지도 동작하도록 방어 코드 있음).

---

## 8. 카드·그리드 (대표 패턴)

- **서비스 카드:** `.service-card`, 호버 시 살짝 떠오름 + 테두리 밝아짐. 오버레이 `::before`는 `pointer-events: none` 처리됨.
- **2열 그리드:** `.services__grid` — `grid-template-columns: repeat(2, 1fr)`, `gap: 20px`.
- **컴팩트 카드:** `.service-card--compact` — 패딩·타이틀 크기 살짝 축소.
- **강조 카드:** `.service-card--featured` — 그라데이션 배경만 (레이아웃 2×2용으로 행 스팬은 제거된 상태).

다른 페이지에 “같은 느낌의 카드”를 만들 때는 위 클래스를 재사용하거나, 배경 `var(--bg-card)` + `border-radius: var(--radius-lg)` + `border: 1px solid var(--border-soft)` 조합을 맞추면 됩니다.

---

## 9. 폼 (문의 블록)

- `.contact__form` — 카드형 배경, `border-radius: var(--radius-xl)`.
- `.form__group`, `.form__row` — 라벨 + 입력. 입력 필드는 다크톤 배경에 얇은 테두리.
- 스팸 허니팟: `.form__hp` (시각적으로 숨김).

---

## 10. 스크롤 리빌 (선택)

`main.js`가 `.pain-card`, `.service-card`, `.tech-card` 등에 `.reveal`을 붙이고, 교차 시 `.visible`을 추가합니다. 새 페이지에서 같은 애니메이션을 쓰려면:

1. `main.js`의 `addReveal('셀렉터')`에 새 섹션 선택자를 추가하거나,
2. 직접 요소에 `.reveal` 클래스를 주고 스크롤 옵저버를 연결합니다.

---

## 11. 체크리스트 — 다른 사이트에 이 디자인만 가져갈 때

1. [ ] `styles.css` 복사(또는 공통 CDN/빌드에 포함).
2. [ ] Google Fonts 링크 동일하게 로드.
3. [ ] `:root` 변수 수정 없이 쓰면 색이 100% 일치.
4. [ ] `.container` + `.section-header` 패턴으로 섹션 제목 통일.
5. [ ] CTA는 `.btn.btn--primary` 또는 맥락에 맞게 `.btn--ghost` / `.btn--white`.
6. [ ] 고정 네비 사용 시 첫 콘텐츠에 상단 여백 확보.
7. [ ] `main.js` 필요 여부: 네비 스크롤·모바일 메뉴·폼 AJAX·캔버스 히어로는 각각 독립 — 없는 기능은 해당 블록만 제외해도 됨.

---

## 12. 파일 참조

| 파일 | 역할 |
|------|------|
| `styles.css` | 전역 스타일 단일 소스 |
| `index.html` | 메인 랜딩 구조·섹션 순서 참고 |
| `socialtwin-detail.html` | 서브 페이지·네비 링크 패턴 참고 |
| `main.js` | 네비, 스무스 스크롤(`a[href^="#"]`), 폼, 히어로 캔버스, 리빌 |

---

## 13. 버전

- 문서 기준: `styles.css`에 `?v=2` 쿼리로 캐시 무효화 중. 큰 디자인 변경 시 이 MD의 “섹션 번호”와 변수 표를 함께 갱신하는 것을 권장합니다.
