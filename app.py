"""
AI Social Twin - 가상인구 생성 및 조사 설계 애플리케이션
"""
from __future__ import annotations

import os
import re
import time
import traceback
import pickle
import json
from typing import List, Dict, Any, Optional, Tuple, TYPE_CHECKING
from io import BytesIO
from datetime import datetime

import gc
import streamlit as st
import pandas as pd
import numpy as np

# 타입 검사용 (실제 로딩은 main()에서 지연 로딩)
if TYPE_CHECKING:
 from google import genai
 from utils.kosis_client import KosisClient
 from utils.ipf_generator import generate_base_population
 from utils.gemini_client import GeminiClient
 from utils.step2_records import STEP2_RECORDS_DIR, list_step2_records, save_step2_record

from core.constants import (
 APP_TITLE,
 AXIS_MARGIN_BACKUP_PATH,
 get_autosave_path,
 get_autosave_metadata_path,
 CACHE_TTL_SECONDS,
 EXPORT_SHEET_NAME,
 EXPORT_COLUMNS,
 STEP2_COLUMN_RENAME,
 DEFAULT_WEIGHTS_SCORE,
 SIDO_MASTER,
 SIDO_CODE,
 SIDO_NAME,
 SIDO_LABELS,
 SIDO_LABEL_TO_CODE,
 SIDO_CODE_TO_NAME,
 AXIS_KEYS,
 AXIS_LABELS,
)
from generate_logic import (
 get_cached_kosis_json,
 fetch_kosis_raw_structure,
 convert_kosis_to_distribution_cached,
 convert_kosis_to_distribution_impl,
 hash_dataframe,
 cached_generate_base_population,
 apply_step2_column_rename,
 blank_unapplied_axis_columns,
 build_excel_bytes_for_download,
)
from generate_logic.step2_logic import (
 get_step2_target_distributions,
 build_step2_error_report,
 apply_step2_row_consistency,
 apply_step2_logical_consistency_cached,
)
from ui import fragment_result_tabs, fragment_draw_charts, render_validation_tab, calculate_mape
from core.db import (
 db_init,
 db_upsert_stat,
 db_delete_stat_by_id,
 db_update_stat_by_id,
 db_get_all_axis_margin_stats,
 db_upsert_axis_margin_stat,
 db_upsert_template,
 build_stats_template_xlsx_kr,
 build_stats_export_xlsx_kr,
 get_export_filename,
 import_stats_from_excel_kr,
 get_sido_vdb_stats,
)
from core.session_cache import (
 get_cached_db_list_stats,
 get_cached_db_axis_margin_stats,
 get_cached_db_six_axis_stat_ids,
 invalidate_db_stats_cache,
 invalidate_db_axis_margin_cache,
)
from pages.stats_preprocess import page_stats_preprocess

# -----------------------------
# 4. UI Utilities
# -----------------------------
def group_by_category(stats: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
 out: Dict[str, List[Dict[str, Any]]] = {}
 for s in stats:
 out.setdefault(s["category"], []).append(s)
 return out


_SESSION_DEFAULTS = {
 "app_started": False,
 "generated_df": None,
 "report": None,
 "sigungu_list": ["전체"],
 "selected_categories": [],
 "selected_stats": [],
 "last_error": None,
 "selected_sido_label": "경상북도 (37)",
 "generate_state_by_sido": {}, # 시도별 1·2단계 상태 완전 분리 {sido_code: {...}}
 "last_generated_sido_code": "", # 1단계 생성 완료 시 설정 → 우측 결과 패널에서 이 시도 state 우선 사용
}

# 시도별 생성 상태의 기본 키들 (한 지역 수정 시 다른 지역에 반영되지 않도록)
_GEN_STATE_DEFAULT_KEYS = {
 "step1_df": None,
 "step2_df": None,
 "generated_df": None,
 "step1_completed": False,
 "generated_n": 0,
 "generated_sido_code": "",
 "generated_sido_name": "",
 "generated_weights": {},
 "generated_report": "",
 "step1_margins_axis": {},
 "generated_margins_axis": {},
 "step2_added_columns": [],
 "step2_validation_info": [],
 "generated_excel": None,
 "show_step2_dialog": False,
 "step1_error_report": None,
 "step1_final_mae": None,
 "step1_edu_diagnostic_detail": [],
 "step1_debug_log": "",
}


def _get_generate_state(sido_code: str) -> dict:
 """해당 시도의 1·2단계 생성 상태 반환. 없으면 기본 구조 생성 후 반환 (다른 지역과 완전 분리)."""
 if "generate_state_by_sido" not in st.session_state:
 st.session_state["generate_state_by_sido"] = {}
 by_sido = st.session_state["generate_state_by_sido"]
 if sido_code not in by_sido:
 by_sido[sido_code] = dict(_GEN_STATE_DEFAULT_KEYS)
 state = by_sido[sido_code]
 for k, v in _GEN_STATE_DEFAULT_KEYS.items():
 if k not in state:
 state[k] = v
 return state


def _get_current_gen_sido_code() -> str:
 """가상인구 생성 탭에서 현재 선택된 시도 코드. URL ?sido= 또는 지도 선택(gen_sido) 또는 기본값."""
 qp = st.query_params
 url_sido = qp.get("sido")
 if url_sido and url_sido in SIDO_CODE:
 return url_sido
 gen_sido_label = st.session_state.get("gen_sido")
 if gen_sido_label and "(" in str(gen_sido_label):
 code = str(gen_sido_label).split("(")[-1].rstrip(")").strip()
 if code in SIDO_CODE:
 return code
 return SIDO_LABEL_TO_CODE.get(SIDO_LABELS[0], "37")


def ensure_session_state():
 for key, default in _SESSION_DEFAULTS.items():
 if key not in st.session_state:
 st.session_state[key] = default


# -----------------------------
# 5. KOSIS Helpers
# -----------------------------

def load_sigungu_options(sido_code: str, kosis_client: KosisClient) -> List[str]:
 """
 KOSIS 인구 테이블에서 시군구 목록 추출
 """
 try:
 # kosis_client가 리스트로 잘못 전달된 경우 방어 코드
 if isinstance(kosis_client, list):
 st.warning("kosis_client 타입 오류")
 return []
 
 url = (
 f"https://kosis.kr/openapi/statisticsData.do?"
 f"method=getList&apiKey=YOUR_KEY&format=json&jsonVD=Y&"
 f"userStatsId=...&objL1={sido_code}"
 )
 data = get_cached_kosis_json(url)
 sigungu_list = kosis_client.extract_sigungu_list_from_population_table(
 data, sido_prefix=SIDO_CODE_TO_NAME.get(sido_code, "")
 )
 return sigungu_list
 except Exception as e:
 st.warning(f"시군구 목록 로드 실패: {e}")
 return []



def _read_axis_margin_backup() -> dict:
 """로컬 백업 파일에서 시도별 6축 설정 로드. {sido_code: {axis_key: stat_id}}"""
 try:
 if os.path.isfile(AXIS_MARGIN_BACKUP_PATH):
 with open(AXIS_MARGIN_BACKUP_PATH, "r", encoding="utf-8") as f:
 return json.load(f)
 except Exception:
 pass
 return {}


def _write_axis_margin_backup(sido_code: str, axis_key_to_stat_id: dict) -> None:
 """해당 시도의 6축 설정을 로컬 백업에 병합 저장. axis_key_to_stat_id: {axis_key: stat_id}."""
 try:
 all_data = _read_axis_margin_backup()
 all_data[str(sido_code)] = {k: int(v) for k, v in axis_key_to_stat_id.items() if v is not None}
 with open(AXIS_MARGIN_BACKUP_PATH, "w", encoding="utf-8") as f:
 json.dump(all_data, f, ensure_ascii=False, indent=2)
 except Exception:
 pass


def page_data_management():
 """데이터 관리 페이지"""
 st.title("데이터 관리")

 # 새로고침 후에도 같은 시도가 보이도록 URL 쿼리 파라미터와 동기화
 SIDO_CODE_TO_LABEL = {v: k for k, v in SIDO_LABEL_TO_CODE.items()}
 _url_sido = st.query_params.get("sido")
 if _url_sido and st.session_state.get("data_mgmt_sido") is None:
 if _url_sido in SIDO_CODE_TO_LABEL:
 st.session_state["data_mgmt_sido"] = SIDO_CODE_TO_LABEL[_url_sido]
 if st.session_state.get("data_mgmt_sido") not in SIDO_LABELS:
 st.session_state["data_mgmt_sido"] = SIDO_LABELS[0]

 sido_label = st.selectbox(
 "시도 선택",
 options=SIDO_LABELS,
 key="data_mgmt_sido",
 )
 sido_code = SIDO_LABEL_TO_CODE[sido_label]
 sido_name = SIDO_CODE_TO_NAME[sido_code]

 # 선택한 시도와 페이지를 URL에 반영해 새로고침 시 가상인구 생성·해당 시도로 복원 가능하게
 try:
 qp = st.query_params
 if qp.get("sido") != sido_code or qp.get("page") != "generate":
 qp["page"] = "generate"
 qp["sido"] = sido_code
 except Exception:
 pass

 st.markdown("---")
 st.subheader("통계 목록")

 # 통계 목록 업로드/다운로드
 col1, col2 = st.columns(2)
 with col1:
 if st.button("템플릿 다운로드", key="download_stats_template"):
 template_bytes = build_stats_template_xlsx_kr(sido_code)
 st.download_button(
 "통계목록 템플릿.xlsx 다운로드",
 data=template_bytes,
 file_name=f"{sido_name}_통계목록_템플릿.xlsx",
 mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
 )

 if st.button("활성화 통계 내보내기", key="export_active_stats"):
 export_bytes = build_stats_export_xlsx_kr(sido_code)
 st.download_button(
 "통계목록.xlsx 다운로드",
 data=export_bytes,
 file_name=f"{sido_name}_통계목록_활성화.xlsx",
 mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
 )

 with col2:
 uploaded_file = st.file_uploader(
 "통계 목록 업로드(.xlsx)",
 type=["xlsx"],
 key="upload_stats_list",
 )
 if uploaded_file:
 file_bytes = uploaded_file.read()
 result = import_stats_from_excel_kr(sido_code, file_bytes)
 if not result.get("ok", True):
 st.error(result.get("error", "업로드 실패"))
 else:
 st.success(
 f"통계 목록 업로드 완료\n"
 f"- 반영: {result.get('반영건수', 0)}건\n"
 f"- 스킵: {result.get('스킵건수', 0)}건\n"
 f"- 오류: {result.get('오류건수', 0)}건"
 )
 if result.get("오류상세"):
 with st.expander("오류 상세"):
 st.json(result["오류상세"])
 st.rerun()

 # 통계 목록 표시
 st.markdown("---")
 all_stats = get_cached_db_list_stats(sido_code)
 if not all_stats:
 st.info("등록된 통계가 없습니다.")
 else:
 df_stats = pd.DataFrame(all_stats)
 df_stats["is_active"] = df_stats["is_active"].map({1: "Y", 0: "N"})
 st.dataframe(df_stats, use_container_width=True)

 # 인구통계 기본 소스(6축 마진) 설정 — 저장 후 새로고침해도 항상 DB에서 직접 조회해 표시
 st.markdown("---")
 st.subheader("인구통계 기본 소스")
 st.markdown("각 축의 목표 마진을 제공할 통계를 선택한 뒤 **「인구통계 기본 소스 저장」** 버튼을 누르세요.")
 st.caption("💡 **교육정도**가 1차 생성에 반영되려면 반드시 **교육 (edu)** 축에 KOSIS 교육 통계를 선택해 저장하세요. 저장 후 새로고침 시 위 **시도 선택**과 주소창 `?sido=코드`가 같으면 선택한 축이 유지됩니다.")

 active_stats = [s for s in all_stats if s["is_active"] == 1]
 if not active_stats:
 st.info("활성화된 통계가 없습니다.")
 else:
 # id를 int로 통일해 Supabase 반환값(문자열 등)과 비교 오류 방지
 stat_options = {int(s["id"]): f"[{s['category']}] {s['name']}" for s in active_stats}

 # 저장된 6축 설정을 새로고침 후에도 항상 표시하기 위해 DB에서 직접 조회(세션 캐시 사용 안 함)
 axis_margin_by_key = db_get_all_axis_margin_stats(sido_code)
 # DB가 비어 있으면 로컬 백업에서 복원 (Supabase 미연결/RLS 시에도 선택값 유지)
 backup_used = False
 if not axis_margin_by_key or all(
 (r.get("stat_id") is None for r in axis_margin_by_key.values())
 ):
 backup_data = _read_axis_margin_backup()
 sido_backup = backup_data.get(str(sido_code), {})
 if sido_backup:
 axis_margin_by_key = {
 str(ak): {"stat_id": sid, "sido_code": sido_code, "axis_key": ak}
 for ak, sid in sido_backup.items()
 }
 backup_used = True
 if backup_used:
 st.info("📁 **로컬 백업**에서 인구통계 기본 소스를 복원했습니다. Supabase에 저장·조회가 되지 않는 경우 이 값이 표시됩니다. 저장 시 백업도 함께 갱신됩니다.")

 # 저장된 stat_id 중 활성 목록에 없어도 선택지에 포함해 "선택 안 함"으로 빠지지 않게 함
 all_stats_by_id = {int(s["id"]): s for s in all_stats if s.get("id") is not None}
 for axis_key, row in axis_margin_by_key.items():
 sid = row.get("stat_id")
 if sid is None:
 continue
 try:
 sid_int = int(sid)
 except (TypeError, ValueError):
 continue
 if sid_int not in stat_options and sid_int in all_stats_by_id:
 s = all_stats_by_id[sid_int]
 stat_options[sid_int] = f"[비활성] [{s.get('category','')}] {s.get('name','')}"

 option_list = [None] + list(stat_options.keys())
 # 지역별로 데이터 관리 축이 다름 (예: 대구는 직업분류 제외) — regions에서 조회
 from regions import get_data_management_axes
 axis_list = get_data_management_axes(sido_code)

 selections = {}
 load_failed_count = 0
 for axis_key, axis_label in axis_list:
 current_stat = axis_margin_by_key.get(axis_key)
 current_id = None
 if current_stat and current_stat.get("stat_id") is not None:
 try:
 current_id = int(current_stat["stat_id"])
 except (TypeError, ValueError):
 current_id = None
 else:
 load_failed_count += 1
 if current_id is not None and current_id not in option_list:
 current_id = None
 default_idx = 0 if current_id is None else (option_list.index(current_id) if current_id in option_list else 0)

 selected_id = st.selectbox(
 f"{axis_label} ({axis_key})",
 options=option_list,
 format_func=lambda x, opts=stat_options: "선택 안 함" if x is None else opts.get(x, "?"),
 index=default_idx,
 key=f"axis_margin_{axis_key}",
 )
 selections[axis_key] = selected_id

 # DB에는 있는데 6축이 전부 조회되지 않으면 RLS(권한) 문제일 수 있음
 if load_failed_count >= 6:
 st.info(
 "💡 **6축 설정이 DB에 있는데도 표시되지 않나요?** "
 "Supabase 대시보드 → **SQL Editor**에서 프로젝트의 **docs/SUPABASE_RLS_정책_적용.sql** 내용을 실행하세요. "
 "anon 역할에 SELECT가 허용되어야 앱에서 조회됩니다."
 )

 if st.button("인구통계 기본 소스 저장", type="primary", key="save_six_axis"):
 updated = 0
 axis_key_to_stat_id = {}
 for axis_key, axis_label in axis_list:
 sid = selections.get(axis_key)
 if sid is not None:
 axis_key_to_stat_id[axis_key] = sid
 try:
 db_upsert_axis_margin_stat(sido_code, axis_key, int(sid))
 updated += 1
 except Exception as e:
 st.error(f"{axis_label} 저장 실패: {e}")
 if updated:
 invalidate_db_axis_margin_cache(sido_code)
 invalidate_db_stats_cache(sido_code)
 invalidate_db_stats_cache("00")
 _write_axis_margin_backup(sido_code, axis_key_to_stat_id)
 try:
 st.query_params["page"] = "generate"
 st.query_params["sido"] = sido_code
 except Exception:
 pass
 st.success(f"인구통계 기본 소스 {updated}건 저장되었습니다. DB와 로컬 백업에 반영되었으며, 새로고침 시에도 유지됩니다.")
 st.rerun()


# -----------------------------
# 7. Pages: 생성(좌 옵션 / 우 결과)
# -----------------------------
def page_generate():
 st.title("가상인구 생성")
 current_sido_code = _get_current_gen_sido_code()
 state = _get_generate_state(current_sido_code)

 # ========== 오토세이브 로딩 로직 (현재 시도 전용, 다른 지역과 분리) ==========
 autosave_path = get_autosave_path(current_sido_code)
 metadata_path = get_autosave_metadata_path(current_sido_code)
 if state.get("step1_df") is None and os.path.exists(autosave_path):
 try:
 step1_df = pd.read_pickle(autosave_path)
 state["step1_df"] = step1_df
 state["step1_completed"] = True
 if os.path.exists(metadata_path):
 with open(metadata_path, "rb") as f:
 meta = pickle.load(f)
 for key, value in meta.items():
 if key in _GEN_STATE_DEFAULT_KEYS and (state.get(key) is None or state.get(key) == _GEN_STATE_DEFAULT_KEYS.get(key)):
 state[key] = value
 if "margins_axis" in meta:
 state["step1_margins_axis"] = meta["margins_axis"]
 state["generated_margins_axis"] = meta["margins_axis"]
 st.info("이전 생성 결과를 자동으로 불러왔습니다.")
 except Exception as e:
 st.warning(f"오토세이브 파일 로드 실패: {e}")

 # 좌우 2단 레이아웃 (0.35:0.65 비율)
 col_left, col_right = st.columns([0.35, 0.65])

 # ========== 좌측: 생성 옵션 ==========
 with col_left:
 st.subheader("생성 옵션")
 
 # 초기화 버튼 (현재 선택된 시도만 초기화, 다른 지역에는 영향 없음)
 if st.button("결과 초기화", type="secondary", use_container_width=True):
 for k in _GEN_STATE_DEFAULT_KEYS:
 state[k] = _GEN_STATE_DEFAULT_KEYS[k]
 if current_sido_code == st.session_state.get("last_generated_sido_code"):
 st.session_state["last_generated_sido_code"] = ""
 _ap = get_autosave_path(current_sido_code)
 _mp = get_autosave_metadata_path(current_sido_code)
 if os.path.exists(_ap):
 try:
 os.remove(_ap)
 except Exception:
 pass
 if os.path.exists(_mp):
 try:
 os.remove(_mp)
 except Exception:
 pass
 st.success("초기화 완료")
 st.rerun()
 
 st.markdown("---")

 # 지도: 가상인구 DB와 동일 Choropleth, 지도 클릭 시 시도 선택과 양방향 연동
 from pages.virtual_population_db import _build_korea_choropleth_figure
 _gen_sido_default = f"{list(SIDO_CODE.values())[0]} ({list(SIDO_CODE.keys())[0]})"
 # 지도 클릭 선택 처리 → gen_sido 갱신 (양방향 바인딩)
 _map_state = st.session_state.get("gen_sido_map")
 _sel = None
 if _map_state is not None:
 _sel = _map_state.get("selection") if isinstance(_map_state, dict) else getattr(_map_state, "selection", None)
 _pts = []
 if _sel is not None:
 _pts = _sel.get("points", []) if isinstance(_sel, dict) else (getattr(_sel, "points", None) or [])
 if not _pts and isinstance(_sel, dict) and _sel.get("locations"):
 _pts = [{"location": loc} if isinstance(loc, (str, int, float)) else loc for loc in (_sel.get("locations") or [])]
 if _pts:
 _p0 = _pts[0] if isinstance(_pts[0], dict) else (getattr(_pts[0], "__dict__", None) or {})
 _cd = _p0.get("customdata") or _p0.get("customData")
 _loc_id = _p0.get("location")
 _code = None
 if _cd and (isinstance(_cd, (list, tuple)) and len(_cd) > 0):
 _code = str(_cd[0])
 elif isinstance(_cd, (str, int, float)):
 _code = str(_cd)
 if not _code and _loc_id is not None:
 _code = str(_loc_id)
 if not _code:
 _pi = _p0.get("point_index") or _p0.get("pointIndex")
 if _pi is not None:
 _sidos_ordered = [s["sido_code"] for s in SIDO_MASTER if s["sido_code"] != "00"]
 if 0 <= _pi < len(_sidos_ordered):
 _code = str(_sidos_ordered[_pi])
 if _code and _code in SIDO_CODE_TO_NAME:
 st.session_state["gen_sido"] = f"{SIDO_CODE_TO_NAME[_code]} ({_code})"
 _gen_sido_label = st.session_state.get("gen_sido", _gen_sido_default)
 _gen_sido_code = _gen_sido_label.split("(")[-1].rstrip(")").strip()
 _region_stats = get_sido_vdb_stats()
 _gen_fig = _build_korea_choropleth_figure(_gen_sido_code, _region_stats)
 st.plotly_chart(_gen_fig, key="gen_sido_map", use_container_width=True, on_select="rerun", selection_mode="points")

 # 1) 시도 선택
 selected_label = st.selectbox(
 "시도 선택",
 options=[f"{v} ({k})" for k, v in SIDO_CODE.items()],
 key="gen_sido",
 )
 sido_code = selected_label.split("(")[-1].rstrip(")")
 sido_name = SIDO_CODE[sido_code]

 # 2) 생성 인구수
 n = st.number_input(
 "생성 인구수",
 min_value=10,
 max_value=100000,
 value=1000,
 step=100,
 )

 # 3) 인구통계 기본소스 가중치
 with st.expander("**인구통계 기본소스 가중치**", expanded=True):
 w_sigungu = st.slider("시군구", 0.0, 5.0, 1.0, key="w_sigungu")
 w_gender = st.slider("성별", 0.0, 5.0, 1.0, key="w_gender")
 w_age = st.slider("연령", 0.0, 5.0, 1.0, key="w_age")
 w_econ = st.slider("경제활동", 0.0, 5.0, 1.0, key="w_econ")
 w_income = st.slider("소득", 0.0, 5.0, 1.0, key="w_income")
 w_edu = st.slider("교육정도", 0.0, 5.0, 1.0, key="w_edu")
 w_job = st.slider("직업분류", 0.0, 5.0, 1.0, key="w_job")
 st.caption("가중치를 높이면 해당 축 분포가 KOSIS 통계에 더 가깝게 반영됩니다.")

 # 4) 통계 목표 활성화 (UI만 유지)
 st.markdown("**통계 목표 활성화**")
 active_stats = [s for s in get_cached_db_list_stats(sido_code) if s["is_active"] == 1]
 if not active_stats:
 st.info("활성화된 통계가 없습니다.")
 else:
 stat_options = {s["id"]: f"[{s['category']}] {s['name']}" for s in active_stats}
 st.multiselect(
 "목표로 할 통계 선택 (선택 사항)",
 options=list(stat_options.keys()),
 format_func=lambda x: stat_options[x],
 )

 # 5) 생성 버튼
 if st.button("가상인구 생성", type="primary", key="btn_gen_pop"):
 import io
 import contextlib
 import traceback
 from datetime import datetime

 log_buf = io.StringIO()
 def log(msg: str):
 log_buf.write(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}\n")
 log_buf.flush()

 try:
 log("버튼 클릭됨 – 생성 시작")
 gen_state = _get_generate_state(sido_code)
 gen_state["step1_debug_log"] = log_buf.getvalue()

 with st.spinner("KOSIS 통계 수집 및 인구 생성 중… (1~2분 소요될 수 있음)"):
 with contextlib.redirect_stdout(log_buf):
 print("[1단계] 생성 시작 (stdout 캡처 중)")
 # KOSIS 클라이언트 초기화
 log("KOSIS 클라이언트 초기화 중...")
 kosis = KosisClient(use_gemini=False)
 print("[1단계] KOSIS 클라이언트 준비 완료")

 # 6축 마진 통계 소스에서 KOSIS 데이터 가져와서 확률 분포로 변환 (통계 목록 1회만 조회)
 with st.spinner("KOSIS 통계 데이터 가져오는 중..."):
 from regions import get_generation_axis_keys
 axis_keys = get_generation_axis_keys(sido_code)
 margins_axis = {}
 # 해당 시도 + 전국(00) 통계 병합: 교육 등이 전국으로만 등록된 경우에도 stat_id 조회 가능
 _stats_sido = get_cached_db_list_stats(sido_code)
 _stats_00 = get_cached_db_list_stats("00") if sido_code != "00" else []
 _seen_ids = {int(s["id"]) for s in _stats_sido if s.get("id") is not None}
 all_stats = _stats_sido + [s for s in _stats_00 if s.get("id") is not None and int(s["id"]) not in _seen_ids]
 edu_diagnostic_lines = []
 for axis_key in axis_keys:
 margin_stat = get_cached_db_axis_margin_stats(sido_code, axis_key)
 want_id = None
 if margin_stat and margin_stat.get("stat_id") is not None:
 try:
 want_id = int(margin_stat["stat_id"])
 except (TypeError, ValueError):
 pass
 # DB에 없으면 로컬 백업에서 stat_id 사용 (데이터 관리에서 저장한 인구통계 기본 소스와 동일하게)
 if want_id is None:
 backup_data = _read_axis_margin_backup()
 sido_backup = backup_data.get(str(sido_code), {})
 if axis_key in sido_backup and sido_backup[axis_key] is not None:
 try:
 want_id = int(sido_backup[axis_key])
 except (TypeError, ValueError):
 pass
 # edu 진단용 상세 로그 (근본 원인 확인) — 진단 페이지에서 바로 볼 수 있도록 리스트에도 저장
 if axis_key == "edu":
 _ed1 = f"sido_code={sido_code!r}, DB stat_id={margin_stat.get('stat_id') if margin_stat else None}, backup want_id={want_id}"
 log(f"[edu 진단] {_ed1}")
 edu_diagnostic_lines.append(_ed1)
 if want_id is not None:
 stat_info = next((s for s in all_stats if (s.get("id") is not None and int(s["id"]) == want_id)), None)
 if axis_key == "edu":
 _ed2 = f"stat_info 찾음={stat_info is not None}, all_stats 수={len(all_stats)} (시도+전국)"
 log(f"[edu 진단] {_ed2}")
 edu_diagnostic_lines.append(_ed2)
 if stat_info:
 st.info(f"{axis_key} <- [{stat_info['category']}] {stat_info['name']}")
 try:
 kosis_data = get_cached_kosis_json(stat_info["url"])
 # edu: 원인 파악을 위해 KOSIS 원본을 항상 진단에 기록 (빈 값이어도 타입·길이·첫 행 표시)
 if axis_key == "edu":
 _ty = type(kosis_data).__name__
 if isinstance(kosis_data, list):
 _len = len(kosis_data)
 edu_diagnostic_lines.append(f"KOSIS 원본: 타입=list, 길이={_len}")
 if _len == 0:
 edu_diagnostic_lines.append("→ 빈 목록이라 변환 불가. 아래 'API 원본(캐시 우회)'로 실제 반환 구조 확인.")
 _empty_struct = st.session_state.pop("_kosis_last_empty_structure", None)
 if _empty_struct:
 edu_diagnostic_lines.append(f"API 반환 구조(참고): {_empty_struct}")
 try:
 raw_struct = fetch_kosis_raw_structure(stat_info["url"])
 edu_diagnostic_lines.append(f"API 원본(캐시 우회 1회): {raw_struct}")
 except Exception as _e:
 edu_diagnostic_lines.append(f"API 원본 조회 실패: {_e!r}")
 else:
 first = kosis_data[0]
 if isinstance(first, dict):
 edu_diagnostic_lines.append(f"첫 행 키: {list(first.keys())[:25]}")
 edu_diagnostic_lines.append(f"첫 행 값 샘플: {dict(list(first.items())[:10])}")
 else:
 edu_diagnostic_lines.append(f"첫 행 타입: {type(first).__name__}, 값: {str(first)[:300]}")
 elif isinstance(kosis_data, dict):
 edu_diagnostic_lines.append(f"KOSIS 원본: 타입=dict, 키={list(kosis_data.keys())[:15]}")
 for _k in ("data", "RESULT", "Grid", "items"):
 v = kosis_data.get(_k)
 if v is not None:
 edu_diagnostic_lines.append(f" {_k}: 타입={type(v).__name__}, len={len(v) if isinstance(v, (list, dict)) else 'N/A'}")
 else:
 edu_diagnostic_lines.append(f"KOSIS 원본: 타입={_ty}, 값: {str(kosis_data)[:200]}")
 if axis_key == "edu" and str(sido_code) == "11":
 labels, probs = convert_kosis_to_distribution_impl(kosis_data, axis_key, sido_code=sido_code)
 else:
 labels, probs = convert_kosis_to_distribution_cached(
 json.dumps(kosis_data, sort_keys=True, default=str), axis_key, sido_code=sido_code
 )
 if labels and probs:
 margins_axis[axis_key] = {"labels": labels, "p": probs}
 st.success(f"{axis_key}: {len(labels)}개 항목 ({sum(probs):.2f} 확률 합)")
 else:
 if axis_key == "edu":
 _ed3 = f"KOSIS 변환 결과 빈값 — labels={len(labels) if labels else 0}개, probs={len(probs) if probs else 0}개"
 log(f"[edu 진단] {_ed3}")
 edu_diagnostic_lines.append(_ed3)
 raw = kosis_data
 if isinstance(raw, dict):
 for _k in ("data", "RESULT", "Result", "Grid", "items"):
 if isinstance(raw.get(_k), list) and raw[_k]:
 raw = raw[_k]
 break
 if isinstance(raw, list) and len(raw) > 0:
 first = raw[0]
 if isinstance(first, dict):
 edu_diagnostic_lines.append(f"KOSIS 첫 행 키: {list(first.keys())[:20]}")
 edu_diagnostic_lines.append(f"KOSIS 첫 행 값 샘플: {dict(list(first.items())[:8])}")
 else:
 edu_diagnostic_lines.append(f"KOSIS 첫 행 타입: {type(first).__name__}, 값: {str(first)[:200]}")
 st.warning(f"{axis_key}: KOSIS 데이터 변환 실패 (균등 분포 사용)")
 except Exception as e:
 if axis_key == "edu":
 _ed4 = f"KOSIS 가져오기/변환 예외: {e!r}"
 log(f"[edu 진단] {_ed4}")
 edu_diagnostic_lines.append(_ed4)
 st.warning(f"{axis_key}: KOSIS 데이터 가져오기 실패: {e}")
 elif axis_key == "edu":
 _ed5 = f"stat_id={want_id} 인 통계가 해당 시도+전국 목록에 없음"
 log(f"[edu 진단] {_ed5}")
 edu_diagnostic_lines.append(_ed5)
 elif axis_key == "edu":
 edu_diagnostic_lines.append("DB와 로컬 백업 모두 edu stat_id 없음 — 인구통계 기본 소스에서 교육(edu) 통계를 선택·저장했는지 확인")
 _fail_msg = st.session_state.pop("_edu_fallback_fail_sample", None)
 if _fail_msg:
 edu_diagnostic_lines.append(_fail_msg)
 gen_state["step1_edu_diagnostic_detail"] = edu_diagnostic_lines
 if len(margins_axis) < len(axis_keys):
 st.warning(f"KOSIS 통계 기반: {len(margins_axis)}/{len(axis_keys)} (나머지는 기본값)")
 print(f"[1단계] 마진 수집 완료: {list(margins_axis.keys())}")
 log("마진 수집 완료: " + (", ".join(margins_axis.keys()) if margins_axis else "없음"))
 # 교육정도(edu) 반영 여부 진단용 로그
 if margins_axis and "edu" in margins_axis:
 ed = margins_axis["edu"]
 labels_ed = ed.get("labels", [])
 p_ed = ed.get("p", ed.get("probs", []))
 log(f"교육정도(edu): KOSIS 반영됨 — 항목 수={len(labels_ed)}, 확률합={sum(p_ed):.4f}")
 for lb, p in zip(labels_ed, p_ed):
 log(f" · {lb}: {p:.4f} ({p*100:.2f}%)")
 else:
 log("교육정도(edu): KOSIS 미반영 — 위 [edu 진단] 로그에서 원인 확인 (DB/백업 stat_id, 통계 목록 조회, 변환 결과)")

 # 1단계: KOSIS 통계 기반 6축 인구 생성
 print(f"[1단계] generate_base_population 호출 직전 (n={int(n)})")
 with st.spinner(f"1단계: KOSIS 통계 기반 {int(n)}명 생성 중..."):
 # sigungu_pool 생성 (margins_axis에서 거주지역 목록 추출)
 sigungu_pool = []
 if margins_axis and "sigungu" in margins_axis:
 sigungu_pool = margins_axis["sigungu"].get("labels", [])
 import random
 # 매 실행마다 다른 이름·6축이 나오도록 랜덤 시드 사용 (중복 가상인물 방지)
 base_seed = random.randint(0, 2**31 - 1)
 seed = base_seed
 base_df = cached_generate_base_population(
 n=int(n),
 selected_sigungu_json=json.dumps([], sort_keys=True),
 weights_6axis_json=json.dumps({
 'sigungu': w_sigungu, 'gender': w_gender, 'age': w_age,
 'econ': w_econ, 'income': w_income, 'edu': w_edu, 'job': w_job,
 }, sort_keys=True),
 sigungu_pool_json=json.dumps(sigungu_pool, sort_keys=True),
 seed=seed,
 margins_axis_json=json.dumps(margins_axis if margins_axis else {}, sort_keys=True, default=str),
 apply_ipf_flag=True,
 )

 if base_df is None or base_df.empty:
 st.error("기본 인구 생성 실패")
 st.stop()

 # 오차율 계산 (1회만, Blocking 반복 제거)
 avg_mae_pct = 0.0
 if margins_axis:
 avg_mae, error_report = calculate_mape(base_df, margins_axis)
 avg_mae_pct = avg_mae * 100

 if margins_axis and avg_mae_pct < 5.0:
 st.success(f"KOSIS 통계 기반 {len(base_df)}명 생성 완료 (평균 오차율: {avg_mae_pct:.2f}%)")
 else:
 st.success(f"KOSIS 통계 기반 {len(base_df)}명 생성 완료")
 if margins_axis and avg_mae_pct >= 5.0:
 st.warning(
 f"오차율이 다소 높습니다({avg_mae_pct:.2f}%). "
 "더 정교한 결과를 원하시면 **다시 생성** 버튼을 눌러주세요."
 )

 # Excel은 다운로드 탭에서 요청 시 캐시된 함수로 생성(지연 변환)
 # 시도별 state에만 저장 (다른 지역과 완전 분리)
 gen_state = _get_generate_state(sido_code)
 gen_state["generated_excel"] = None
 gen_state["generated_df"] = base_df
 gen_state["step1_df"] = base_df
 gen_state["step1_completed"] = True
 gen_state["generated_n"] = n
 gen_state["generated_sido_code"] = sido_code
 gen_state["generated_sido_name"] = sido_name
 gen_state["generated_weights"] = {
 "sigungu": w_sigungu,
 "gender": w_gender,
 "age": w_age,
 "econ": w_econ,
 "income": w_income,
 "edu": w_edu,
 "job": w_job,
 }
 gen_state["step1_margins_axis"] = margins_axis if margins_axis else {}
 gen_state["generated_margins_axis"] = margins_axis if margins_axis else {}
 gen_state["generated_report"] = f"KOSIS 통계 기반 {len(base_df)}명 생성 완료 ({len(margins_axis)}축 반영)"
 try:
 base_df.to_pickle(get_autosave_path(sido_code))
 meta = {
 "step1_completed": True,
 "generated_n": n,
 "generated_sido_code": sido_code,
 "generated_sido_name": sido_name,
 "generated_weights": gen_state["generated_weights"],
 "margins_axis": margins_axis if margins_axis else {},
 "generated_report": gen_state["generated_report"],
 }
 with open(get_autosave_metadata_path(sido_code), "wb") as f:
 pickle.dump(meta, f)
 except Exception as e:
 st.warning(f"오토세이브 저장 실패: {e}")

 # ✅ 1단계 생성 완료 시 작업 기록에 6축 정보 저장
 from datetime import datetime
 if "work_logs" not in st.session_state:
 st.session_state.work_logs = []
 
 # 6축+직업분류 정보 추출
 axis_info = {}
 for axis_key in ["sigungu", "gender", "age", "econ", "income", "edu", "job"]:
 if axis_key in margins_axis:
 axis_data = margins_axis[axis_key]
 labels = axis_data.get("labels", [])
 probs = axis_data.get("p", axis_data.get("probs", []))
 axis_info[axis_key] = {
 "labels": labels[:10] if len(labels) > 10 else labels, # 처음 10개만
 "label_count": len(labels),
 "probabilities_sample": probs[:10] if len(probs) > 10 else probs
 }
 
 generation_log = {
 "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
 "stage": "1단계 가상인구 생성 완료",
 "status": "success",
 "sido_code": sido_code,
 "sido_name": sido_name,
 "population_size": len(base_df),
 "target_size": n,
 "weights_6axis": {
 "sigungu": w_sigungu,
 "gender": w_gender,
 "age": w_age,
 "econ": w_econ,
 "income": w_income,
 "edu": w_edu,
 "job": w_job,
 },
 "axis_info": axis_info,
 "axis_count": len(margins_axis)
 }
 st.session_state.work_logs.append(generation_log)
 # 우측 패널이 생성 직후 반드시 이 시도 결과를 보이도록 표시용 시도 코드 저장
 st.session_state["last_generated_sido_code"] = sido_code

 st.success("1단계 완료: KOSIS 통계 기반 생성.")
 st.rerun()

 except Exception as e:
 log_buf.write("\n--- 예외 ---\n")
 log_buf.write(traceback.format_exc())
 st.error(f"생성 실패: {e}")
 st.code(traceback.format_exc())
 finally:
 _get_generate_state(sido_code)["step1_debug_log"] = log_buf.getvalue()

 # ========== 우측: 생성 결과 대시보드 ==========
 with col_right:
 st.subheader("생성 결과 대시보드")
 
 # 표시할 state 결정: 현재 선택 시도 우선, 결과 없으면 방금 생성한 시도(last_generated_sido_code) 사용
 display_state = state
 last_code = st.session_state.get("last_generated_sido_code", "")
 if (state.get("step2_df") is None and state.get("step1_df") is None and state.get("generated_df") is None
 and last_code and last_code in (st.session_state.get("generate_state_by_sido") or {})):
 display_state = _get_generate_state(last_code)

 df = display_state.get("step2_df")
 is_step2 = df is not None
 if df is None:
 df = display_state.get("step1_df")
 if df is None:
 df = display_state.get("generated_df")

 if df is None:
 st.info("👈 왼쪽 패널에서 설정을 마치고 [가상인구 생성] 버튼을 눌러주세요.")
 return

 n = display_state.get("generated_n", len(df))
 sido_name = display_state.get("generated_sido_name", "알 수 없음")
 weights = display_state.get("generated_weights", {})
 report = display_state.get("generated_report", "")
 excel_bytes = display_state.get("generated_excel")
 _sido_code = display_state.get("generated_sido_code", "")
 from regions import get_dashboard_title
 _title = get_dashboard_title(_sido_code, display_state.get("generated_sido_name", ""))
 st.markdown(f"### {_title}")
 col1, col2, col3, col4 = st.columns(4)
 with col1:
 st.metric("총 인구수", f"{len(df):,}명")
 with col2:
 if "성별" in df.columns:
 gender_counts = df["성별"].value_counts()
 male_count = gender_counts.get("남자", gender_counts.get("남", 0))
 male_ratio = (male_count / len(df) * 100) if len(df) > 0 else 0
 st.metric("남성 비율", f"{male_ratio:.1f}%")
 else:
 st.metric("남성 비율", "N/A")
 with col3:
 if "연령" in df.columns:
 avg_age = df["연령"].mean()
 st.metric("평균 연령", f"{avg_age:.1f}세")
 else:
 st.metric("평균 연령", "N/A")
 with col4:
 st.metric("총 컬럼 수", len(df.columns))
 
 st.markdown("---")
 
 # 데이터 미리보기 (2단계 완료 시 대입된 통계 포함)
 if is_step2:
 st.markdown("### 데이터 미리보기 (2단계 완료: 추가 통계 포함)")
 else:
 st.markdown("### 데이터 미리보기")
 
 margins_for_preview = display_state.get("step1_margins_axis") or display_state.get("generated_margins_axis") or {}
 if is_step2:
 df_preview = apply_step2_column_rename(df.copy())
 else:
 df_preview = blank_unapplied_axis_columns(df.copy(), margins_for_preview)
 st.dataframe(
 df_preview.head(100),
 height=300,
 use_container_width=True,
 column_config={
 "페르소나": st.column_config.TextColumn("페르소나", width="large"),
 "현시대 반영": st.column_config.TextColumn("현시대 반영", width="large"),
 },
 )
 
 st.markdown("---")
 
 # ✅ 2단계: 다른 통계 대입 버튼 (항상 표시 - 반복 대입 가능)
 st.markdown("### 2단계: 다른 통계 대입")
 col_step2_1, col_step2_2 = st.columns([3, 1])
 with col_step2_1:
 if is_step2:
 st.info("추가 KOSIS 통계를 반복적으로 대입할 수 있습니다. (이미 대입된 통계 포함)")
 else:
 st.info("1단계에서 생성된 가상인구에 추가 KOSIS 통계를 대입할 수 있습니다.")
 with col_step2_2:
 if st.button("다른 통계 대입", type="primary", use_container_width=True):
 display_state["show_step2_dialog"] = True
 st.rerun()

 if display_state.get("show_step2_dialog", False):
 with st.expander("2단계: 다른 통계 선택 및 대입", expanded=True):
 sido_code = display_state.get("generated_sido_code", "")
 if not sido_code:
 st.error("먼저 1단계 가상인구를 생성해주세요.")
 else:
 # 활성화된 통계 목록 가져오기 (6축 마진에 쓰인 통계는 제외 — 이미 1단계에서 반영됨)
 all_stats = get_cached_db_list_stats(sido_code)
 active_stats = [s for s in all_stats if s.get("is_active", 0) == 1]
 six_axis_stat_ids = get_cached_db_six_axis_stat_ids(sido_code)
 stats_for_step2 = [s for s in active_stats if s["id"] not in six_axis_stat_ids]
 
 if not active_stats:
 st.info("활성화된 통계가 없습니다. 데이터 관리 탭에서 통계를 활성화해주세요.")
 elif not stats_for_step2:
 st.info("2단계에 대입할 통계가 없습니다. (6축에 사용 중인 통계를 제외한 나머지 활성 통계가 없습니다.)")
 else:
 # 통계 선택: 6축 제외한 활성 통계만 표시, 기본값은 전체 선택
 stat_options = {s["id"]: f"[{s['category']}] {s['name']}" for s in stats_for_step2}
 all_stat_ids = list(stat_options.keys())
 
 if six_axis_stat_ids:
 st.caption("6축 마진에 사용 중인 통계는 목록에서 제외됩니다 (이미 1단계에 반영됨).")
 
 # 전체 선택 체크박스 (기본 True — 데이터 관리의 모든 통계 반영)
 select_all = st.checkbox("전체 선택 (6축 제외 나머지 모두 대입)", key="step2_select_all", value=True)
 
 # multiselect 기본값: 전체 선택이면 6축 제외 전부
 default_selection = all_stat_ids if select_all else []
 
 selected_stat_ids = st.multiselect(
 "대입할 통계 선택 (여러 개 선택 가능)",
 options=all_stat_ids,
 default=default_selection,
 format_func=lambda x: stat_options[x],
 key="step2_stat_selection"
 )
 
 # 전체 선택 체크박스 변경 시 자동 반영
 if select_all and len(selected_stat_ids) != len(all_stat_ids):
 selected_stat_ids = all_stat_ids
 st.rerun()
 elif not select_all and len(selected_stat_ids) == len(all_stat_ids):
 pass
 
 col_apply, col_cancel = st.columns(2)
 with col_apply:
 if st.button("통계 대입 실행", type="primary", use_container_width=True):
 if not selected_stat_ids:
 st.warning("통계를 선택해주세요.")
 else:
 with st.spinner("통계 대입 중..."):
 try:
 kosis = KosisClient(use_gemini=True)
 step2_df = display_state.get("step2_df")
 base_df_for_step2 = step2_df if step2_df is not None else df
 result_df = base_df_for_step2.copy()
 
 # 통계별 다열 대입 시 기본 컬럼명 사용 (추가 통계는 Excel 오른쪽에 열로 이어 붙임)
 residence_duration_columns_by_stat = {}
 # 통계 대입 로그 초기화
 if "stat_assignment_logs" not in st.session_state:
 st.session_state.stat_assignment_logs = []
 # 2단계 검증용: 통계별 URL·컬럼·거주기간 여부 저장
 step2_validation_info = []
 
 for stat_id in selected_stat_ids:
 stat_info = next((s for s in active_stats if s["id"] == stat_id), None)
 if stat_info:
 from datetime import datetime
 log_entry = {
 "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
 "stat_id": stat_id,
 "category": stat_info["category"],
 "stat_name": stat_info["name"],
 "url": stat_info.get("url", ""),
 "status": "processing"
 }
 
 try:
 kosis_data = get_cached_kosis_json(stat_info["url"])
 kosis_data_count = len(kosis_data) if isinstance(kosis_data, list) else 0
 log_entry["kosis_data_count"] = kosis_data_count
 
 # KOSIS 데이터 샘플 저장 (최대 5개)
 if isinstance(kosis_data, list) and len(kosis_data) > 0:
 sample_size = min(5, len(kosis_data))
 log_entry["kosis_data_sample"] = kosis_data[:sample_size]
 if len(kosis_data) > 0:
 first_item = kosis_data[0]
 if isinstance(first_item, dict):
 log_entry["kosis_data_fields"] = list(first_item.keys())
 else:
 log_entry["kosis_data_sample"] = []
 log_entry["kosis_data_fields"] = []
 
 # 학생 및 미취학자녀수: 2열(F=자녀 유무, G=자녀 수) 전용 로직
 use_children_student = (
 "학생" in (stat_info.get("name") or "")
 and "미취학" in (stat_info.get("name") or "")
 )
 # 반려동물 현황: 2열(K=유무, L=종류) 전용 로직
 use_pet = "반려동물" in (stat_info.get("name") or "") and "현황" in (stat_info.get("name") or "")
 # 거처 종류 및 점유 형태: 2열(거처 종류, 주택 점유 형태) 전용 로직
 use_dwelling = (
 "거처" in (stat_info.get("name") or "")
 and "점유" in (stat_info.get("name") or "")
 )
 # 부모님 생존여부 및 동거여부: 2열(생존여부, 부모님 동거 여부) 전용 로직
 use_parents_survival_cohabitation = (
 "부모님" in (stat_info.get("name") or "")
 and "생존" in (stat_info.get("name") or "")
 and "동거" in (stat_info.get("name") or "")
 )
 # 부모님 생활비 주 제공자: 단일 컬럼 전용 로직
 use_parents_expense_provider = (
 "부모님" in (stat_info.get("name") or "")
 and "생활비" in (stat_info.get("name") or "")
 and "주 제공자" in (stat_info.get("name") or "")
 )
 # 현재 거주주택 만족도: 3열
 use_housing_satisfaction = (
 "거주" in (stat_info.get("name") or "")
 and "만족도" in (stat_info.get("name") or "")
 and ("주택" in (stat_info.get("name") or "") or "주거" in (stat_info.get("name") or ""))
 )
 # 배우자의 경제활동 상태: 1열 유/무
 use_spouse_economic = (
 "배우자" in (stat_info.get("name") or "")
 and "경제활동" in (stat_info.get("name") or "")
 )
 # 종사상 지위: 1열
 use_employment_status = "종사상 지위" in (stat_info.get("name") or "")
 # 직장명(산업 대분류): 1열
 use_industry_major = "직장명" in (stat_info.get("name") or "") and "산업" in (stat_info.get("name") or "") and "대분류" in (stat_info.get("name") or "")
 # 하는 일의 종류(직업 종분류): 1열
 use_job_class = "하는 일" in (stat_info.get("name") or "") and "직업" in (stat_info.get("name") or "")
 # 취업자 근로여건 만족도: 5열
 use_work_satisfaction = "취업자" in (stat_info.get("name") or "") and "근로여건" in (stat_info.get("name") or "") and "만족도" in (stat_info.get("name") or "")
 # 반려동물 양육비용: 1열 숫자(원)
 use_pet_cost = "반려동물" in (stat_info.get("name") or "") and "양육비용" in (stat_info.get("name") or "")
 # 소득 및 소비생활 만족도: 3열 (소득 여부, 소득 만족도, 소비생활만족도)
 use_income_consumption_satisfaction = (
 "소득" in (stat_info.get("name") or "")
 and "소비생활" in (stat_info.get("name") or "")
 and "만족도" in (stat_info.get("name") or "")
 )
 # 월평균 공교육 및 사교육비: 2열 (공교육비, 사교육비) 만원
 use_education_cost = (
 "공교육" in (stat_info.get("name") or "")
 and "사교육" in (stat_info.get("name") or "")
 )
 # 타지역 소비: 4열
 use_other_region_consumption = (
 "타지역" in (stat_info.get("name") or "")
 and "소비" in (stat_info.get("name") or "")
 )
 # 프리셋 통계 21종 (거주지역 대중교통, 의료기관, 의료시설 만족도 등)
 _sn = (stat_info.get("name") or "")
 use_public_transport_satisfaction = "거주지역 대중교통 만족도" in _sn or ("대중교통" in _sn and "만족도" in _sn)
 use_medical_facility_main = "의료기관 주 이용시설" in _sn or ("의료기관" in _sn and "이용시설" in _sn)
 use_medical_satisfaction = "의료시설 만족도" in _sn or ("의료시설" in _sn and "만족도" in _sn)
 use_welfare_satisfaction = (
 "지역의 사회복지 서비스 만족도" in _sn
 or ("임신" in _sn and "복지" in _sn)
 or ("저소득층" in _sn and "복지" in _sn)
 or ("사회복지" in _sn and "만족도" in _sn)
 )
 use_provincial_satisfaction = "도정만족도" in _sn or "도정정책" in _sn or ("도정" in _sn and "만족도" in _sn) or "행정서비스" in _sn
 use_social_communication = "사회적관계별 소통정도" in _sn or ("사회적" in _sn and "소통" in _sn)
 use_trust_people = "일반인에 대한 신뢰" in _sn or ("일반인" in _sn and "신뢰" in _sn)
 use_subjective_class = "주관적 귀속계층" in _sn or ("주관적" in _sn and "귀속" in _sn)
 use_volunteer = ("자원봉사활동" in _sn or "자원봉사 활동" in _sn or "자원봉사" in _sn) and ("여부" in _sn or "여부및" in _sn or "시간" in _sn)
 use_donation = "후원금 금액" in _sn or "후원금" in _sn or ("기부" in _sn and ("여부" in _sn or "금액" in _sn or "방식" in _sn))
 use_regional_belonging = "지역소속감" in _sn or ("지역" in _sn and "소속감" in _sn) or "동네 소속감" in _sn or "시군 소속감" in _sn
 use_safety_eval = "안전환경에 대한 평가" in _sn or ("안전환경" in _sn and "평가" in _sn) or ("안전" in _sn and "환경" in _sn and "평가" in _sn)
 use_crime_fear = "일상생활 범죄피해 두려움" in _sn or ("일상생활" in _sn and "범죄" in _sn and "두려움" in _sn)
 use_daily_fear = "일상생활에서 두려움" in _sn or ("일상생활" in _sn and "두려움" in _sn and "밤" in _sn)
 use_law_abiding = "자신의 평소 준법수준" in _sn or ("준법" in _sn and "수준" in _sn) or "평소 준법" in _sn
 use_environment_feel = "환경체감도" in _sn or ("환경" in _sn and "체감도" in _sn) or "대기환경" in _sn or "수질환경" in _sn
 use_time_pressure = "생활시간 압박" in _sn or ("생활시간" in _sn and "압박" in _sn)
 use_leisure_satisfaction = (
 ("여가활동 만족도" in _sn and "불만족 이유" in _sn)
 or ("여가활동" in _sn and "불만족" in _sn)
 or "여가활동 만족도 및 불만족 이유" in _sn
 )
 use_culture_attendance = ("문화예술행사" in _sn and "관람" in _sn) or "문화예술행사 관람" in _sn or ("문화예술" in _sn and "관람" in _sn)
 use_life_satisfaction = "삶에 대한 만족감과 정서경험" in _sn or ("삶에 대한" in _sn and "만족감" in _sn) or ("만족감" in _sn and "정서" in _sn)
 use_happiness_level = "행복수준" in _sn or ("행복" in _sn and "수준" in _sn)
 two_cols = residence_duration_columns_by_stat.get(
 (str(stat_info.get("category", "")).strip(), str(stat_info.get("name", "")).strip()), []
 )
 two_names = [r3 for _, r3 in two_cols][:2]
 use_residence_duration = (
 "거주기간" in (stat_info.get("name") or "")
 and "정주의사" in (stat_info.get("name") or "")
 )
 three_cols = residence_duration_columns_by_stat.get(
 (str(stat_info.get("category", "")).strip(), str(stat_info.get("name", "")).strip()), []
 )
 three_names = [r3 for _, r3 in three_cols]
 # 지역별 2단계 프리셋 (서울 등: regions에서 조회)
 _sido = str(display_state.get("generated_sido_code", ""))
 from regions import get_step2_preset_config, get_step2_stat_columns
 preset_config = get_step2_preset_config(_sido)
 stat_columns = get_step2_stat_columns(_sido)
 preset_matched = False
 preset_cols = ()
 if preset_config and stat_columns:
 for key in sorted(preset_config.keys(), key=lambda x: -len(x)):
 if key in _sn:
 preset_cols = stat_columns.get(key, (stat_info.get("name", ""),))
 preset = preset_config.get(key)
 if preset and preset_cols:
 result_df, success = kosis.assign_preset_stat_columns(
 result_df, kosis_data, stat_name=stat_info["name"],
 column_names=preset_cols, preset=preset, seed=42)
 preset_matched = True
 break
 if preset_matched:
 pass
 # 학생 및 미취학자녀수: 가상인구 DB 컬럼명 기준
 elif use_children_student:
 col_f, col_g = (two_names[0], two_names[1]) if len(two_names) >= 2 else ("학생 및 미취학 자녀 유무", "학생 및 미취학 자녀 수")
 result_df, success = kosis.assign_children_student_columns(
 result_df,
 kosis_data,
 column_names=(col_f, col_g),
 seed=42,
 )
 # 반려동물 현황: K·L 2열 (템플릿에 없으면 기본 컬럼명)
 elif use_pet:
 col_k, col_l = (two_names[0], two_names[1]) if len(two_names) >= 2 else ("반려동물유무", "반려동물종류")
 result_df, success = kosis.assign_pet_columns(
 result_df,
 kosis_data,
 column_names=(col_k, col_l),
 seed=42,
 )
 # 거처 종류 및 점유 형태: 2열 (거처 종류, 주택 점유 형태)
 elif use_dwelling:
 col_dw, col_occ = (two_names[0], two_names[1]) if len(two_names) >= 2 else ("거처 종류", "주택 점유 형태")
 result_df, success = kosis.assign_dwelling_columns(
 result_df,
 kosis_data,
 column_names=(col_dw, col_occ),
 seed=42,
 )
 # 부모님 생존여부 및 동거여부: 2열(부모님 생존 여부, 부모님 동거 여부)
 elif use_parents_survival_cohabitation:
 col_survival, col_cohabitation = (two_names[0], two_names[1]) if len(two_names) >= 2 else ("부모님 생존 여부", "부모님 동거 여부")
 result_df, success = kosis.assign_parents_survival_cohabitation_columns(
 result_df,
 kosis_data,
 column_names=(col_survival, col_cohabitation),
 seed=42,
 )
 # 부모님 생활비 주 제공자: 단일 컬럼 (부모님 생존 여부가 해당없음인 행은 해당없음으로 일관 처리)
 elif use_parents_expense_provider:
 col_expense = "부모님 생활비 주 제공자"
 col_survival_for_expense = "부모님 생존 여부"
 if col_survival_for_expense not in result_df.columns:
 col_survival_for_expense = next((c for c in result_df.columns if "생존" in str(c)), None)
 result_df, success = kosis.assign_parents_expense_provider_column(
 result_df,
 kosis_data,
 column_name=col_expense,
 survival_column=col_survival_for_expense,
 seed=42,
 )
 # 현재 거주주택 만족도: 3열
 elif use_housing_satisfaction:
 col_sat1, col_sat2, col_sat3 = (
 "현재 거주 주택 만족도",
 "현재 상하수도, 도시가스 도로 등 기반시설 만족도",
 "주거지역내 주차장이용 만족도",
 )
 result_df, success = kosis.assign_housing_satisfaction_columns(
 result_df,
 kosis_data,
 column_names=(col_sat1, col_sat2, col_sat3),
 seed=42,
 )
 # 배우자의 경제활동 상태: 1열 유/무
 elif use_spouse_economic:
 col_spouse = "배우자의 경제활동 상태"
 result_df, success = kosis.assign_spouse_economic_column(
 result_df,
 kosis_data,
 column_name=col_spouse,
 seed=42,
 )
 # 종사상 지위: 1열
 elif use_employment_status:
 col_emp = "종사상 지위"
 result_df, success = kosis.assign_employment_status_column(
 result_df,
 kosis_data,
 column_name=col_emp,
 seed=42,
 )
 # 직장명(산업 대분류): 1열
 elif use_industry_major:
 col_ind = "직장명(산업 대분류)"
 result_df, success = kosis.assign_industry_major_column(
 result_df,
 kosis_data,
 column_name=col_ind,
 seed=42,
 )
 # 하는 일의 종류(직업 종분류): 1열
 elif use_job_class:
 col_job = "하는 일의 종류(직업 종분류)"
 result_df, success = kosis.assign_job_class_column(
 result_df,
 kosis_data,
 column_name=col_job,
 seed=42,
 )
 # 취업자 근로여건 만족도: 5열 (가상인구 DB 컬럼명 기준)
 elif use_work_satisfaction:
 col_ws = ("하는일 만족도", "임금/가구소득 만족도", "근로시간 만족도", "근무환경 만족도", "근무 여건 전반적인 만족도")
 result_df, success = kosis.assign_work_satisfaction_columns(
 result_df,
 kosis_data,
 column_names=col_ws,
 seed=42,
 )
 # 반려동물 양육비용: 1열 원 단위
 elif use_pet_cost:
 col_pet_cost = "반려동물 양육비용"
 result_df, success = kosis.assign_pet_cost_column(
 result_df,
 kosis_data,
 column_name=col_pet_cost,
 seed=42,
 )
 elif use_income_consumption_satisfaction:
 col_ics = ("소득 여부", "소득 만족도", "소비생활만족도")
 result_df, success = kosis.assign_income_consumption_satisfaction_columns(
 result_df,
 kosis_data,
 column_names=col_ics,
 seed=42,
 )
 elif use_education_cost:
 col_edu = ("공교육비", "사교육비")
 result_df, success = kosis.assign_education_cost_columns(
 result_df,
 kosis_data,
 column_names=col_edu,
 seed=42,
 )
 elif use_other_region_consumption:
 col_other = (
 "경북 외 소비 경험 여부",
 "경북 외 주요 소비지역",
 "경북 외 주요 소비 상품 및 서비스(1순위)",
 "경북 외 주요 소비 상품 및 서비스(2순위)",
 )
 result_df, success = kosis.assign_other_region_consumption_columns(
 result_df,
 kosis_data,
 column_names=col_other,
 seed=42,
 )
 elif use_public_transport_satisfaction:
 result_df, success = kosis.assign_preset_stat_columns(
 result_df, kosis_data, stat_name=stat_info["name"],
 column_names=("시내버스/마을버스 만족도", "시외/고속버스 만족도", "택시 만족도", "기타(기차,선박)만족도"), seed=42)
 elif use_medical_facility_main:
 result_df, success = kosis.assign_preset_stat_columns(
 result_df, kosis_data, stat_name=stat_info["name"],
 column_names=("의료기관 주 이용시설",), seed=42)
 elif use_medical_satisfaction:
 result_df, success = kosis.assign_preset_stat_columns(
 result_df, kosis_data, stat_name=stat_info["name"],
 column_names=("의료시설 만족도",), seed=42)
 elif use_welfare_satisfaction:
 result_df, success = kosis.assign_preset_stat_columns(
 result_df, kosis_data, stat_name=stat_info["name"],
 column_names=("임신·출산·육아에 대한 복지 만족도", "저소득층 등 취약계층에 대한 복지 만족도"), seed=42)
 elif use_provincial_satisfaction:
 result_df, success = kosis.assign_preset_stat_columns(
 result_df, kosis_data, stat_name=stat_info["name"],
 column_names=("도정정책 만족도", "행정서비스 만족도"), seed=42)
 elif use_social_communication:
 result_df, success = kosis.assign_preset_stat_columns(
 result_df, kosis_data, stat_name=stat_info["name"],
 column_names=("사회적관계별 소통정도",), seed=42)
 elif use_trust_people:
 result_df, success = kosis.assign_preset_stat_columns(
 result_df, kosis_data, stat_name=stat_info["name"],
 column_names=("일반인에 대한 신뢰",), seed=42)
 elif use_subjective_class:
 result_df, success = kosis.assign_preset_stat_columns(
 result_df, kosis_data, stat_name=stat_info["name"],
 column_names=("주관적 귀속계층",), seed=42)
 elif use_volunteer:
 result_df, success = kosis.assign_preset_stat_columns(
 result_df, kosis_data, stat_name=stat_info["name"],
 column_names=("자원봉사 활동 여부", "자원봉사 활동 방식", "지난 1년 동안 자원봉사 활동 시간"), seed=42)
 elif use_donation:
 result_df, success = kosis.assign_preset_stat_columns(
 result_df, kosis_data, stat_name=stat_info["name"],
 column_names=("기부 여부", "기부 방식", "기부금액(만원)"), seed=42)
 elif use_regional_belonging:
 result_df, success = kosis.assign_preset_stat_columns(
 result_df, kosis_data, stat_name=stat_info["name"],
 column_names=("동네 소속감", "시군 소속감", "경상북도 소속감"), seed=42)
 elif use_safety_eval:
 result_df, success = kosis.assign_preset_stat_columns(
 result_df, kosis_data, stat_name=stat_info["name"],
 column_names=(
 "(안전환경)어둡고 후미진 곳이 많다", "(안전환경)주변에 쓰레기가 아무렇게 버려져 있고 지저분 하다",
 "(안전환경)주변에 방치된 차나 빈 건물이 많다", "(안전환경)무리 지어 다니는 불량 청소년이 많다",
 "(안전환경)기초질서를 지키지 않는 사람이 많다",
 "(안전환경)큰소리로 다투거나 싸우는 사람들을 자주 볼 수 있다"), seed=42)
 elif use_crime_fear:
 result_df, success = kosis.assign_preset_stat_columns(
 result_df, kosis_data, stat_name=stat_info["name"],
 column_names=("(일상생활 범죄피해 두려움)나자신", "(일상생활 범죄피해 두려움)배우자(애인)", "(일상생활 범죄피해 두려움)자녀", "(일상생활 범죄피해 두려움)부모"), seed=42)
 elif use_daily_fear:
 result_df, success = kosis.assign_preset_stat_columns(
 result_df, kosis_data, stat_name=stat_info["name"],
 column_names=("(일상생활에서 두려움)밤에 혼자 집에 있을 때", "(일상생활에서 두려움)밤에 혼자 지역(동네)의 골목길을 걸을때"), seed=42)
 elif use_law_abiding:
 result_df, success = kosis.assign_preset_stat_columns(
 result_df, kosis_data, stat_name=stat_info["name"],
 column_names=("자신의 평소 준법수준", "평소 법을 지키지 않는 주된 이유"), seed=42)
 elif use_environment_feel:
 result_df, success = kosis.assign_preset_stat_columns(
 result_df, kosis_data, stat_name=stat_info["name"],
 column_names=("대기환경 체감도", "수질환경 체감도", "토양환경 체감도", "소음/진동환경 체감도", "녹지환경 체감도"), seed=42)
 elif use_time_pressure:
 result_df, success = kosis.assign_preset_stat_columns(
 result_df, kosis_data, stat_name=stat_info["name"],
 column_names=("평일 생활시간 압박", "주말 생활시간 압박"), seed=42)
 elif use_leisure_satisfaction:
 result_df, success = kosis.assign_preset_stat_columns(
 result_df, kosis_data, stat_name=stat_info["name"],
 column_names=("문화여가시설 만족도", "전반적인 여가활동 만족도", "여가활동 불만족 이유"), seed=42)
 elif use_culture_attendance:
 result_df, success = kosis.assign_preset_stat_columns(
 result_df, kosis_data, stat_name=stat_info["name"],
 column_names=("문화예술행사 관람 여부", "문화예술행사 관람 분야"), seed=42)
 elif use_life_satisfaction:
 result_df, success = kosis.assign_preset_stat_columns(
 result_df, kosis_data, stat_name=stat_info["name"],
 column_names=(
 "삶에 대한 전반적 만족감(10점 만점)", "살고있는 지역의 전반적 만족감(10점 만점)",
 "어제 행복 정도(10점 만점)", "어제 걱정 정도(10점 만점)"), seed=42)
 elif use_happiness_level:
 result_df, success = kosis.assign_preset_stat_columns(
 result_df, kosis_data, stat_name=stat_info["name"],
 column_names=(
 "생활수준(10점 만점)", "건강상태(10점 만점)", "성취도(10점 만점)", "대인관계(10점 만점)",
 "안전정도(10점 만점)", "지역사회소속감(10점 만점)", "미래안정성(10점 만점)"), seed=42)
 elif use_residence_duration:
 default_names = ("시도 거주기간", "시군구 거주기간", "향후 10년 거주 희망의사")
 if len(three_names) >= 3:
 col1, col2, col3 = three_names[0], three_names[1], three_names[2]
 elif len(three_names) == 2:
 col1, col2, col3 = three_names[0], three_names[1], default_names[2]
 elif len(three_names) == 1:
 col1, col2, col3 = three_names[0], default_names[1], default_names[2]
 else:
 col1, col2, col3 = default_names[0], default_names[1], default_names[2]
 result_df, success = kosis.assign_residence_duration_columns(
 result_df,
 kosis_data,
 column_names=(col1, col2, col3),
 seed=42,
 )
 else:
 from regions import use_slug_fallback_for_unknown_stat
 if use_slug_fallback_for_unknown_stat(_sido):
 result_df, success = kosis.assign_stat_columns_to_population(
 result_df,
 kosis_data,
 category=stat_info["category"],
 stat_name=stat_info["name"],
 url=stat_info["url"]
 )
 else:
 success = False
 
 if success:
 log_entry["status"] = "success"
 log_entry["message"] = "통계 대입 완료"
 st.success(f"[{stat_info['category']}] {stat_info['name']} 대입 완료")
 # 검증 탭에서 KOSIS 대비 검증용 정보 저장
 if preset_matched:
 step2_validation_info.append({
 "stat_name": stat_info.get("name", ""),
 "url": stat_info.get("url", ""),
 "columns": list(preset_cols),
 "is_residence": False,
 "is_preset": True,
 })
 elif use_children_student:
 step2_validation_info.append({
 "stat_name": stat_info.get("name", ""),
 "url": stat_info.get("url", ""),
 "columns": [col_f, col_g],
 "is_residence": False,
 "is_children_student": True,
 })
 elif use_pet:
 step2_validation_info.append({
 "stat_name": stat_info.get("name", ""),
 "url": stat_info.get("url", ""),
 "columns": [col_k, col_l],
 "is_residence": False,
 "is_pet": True,
 })
 elif use_dwelling:
 step2_validation_info.append({
 "stat_name": stat_info.get("name", ""),
 "url": stat_info.get("url", ""),
 "columns": [col_dw, col_occ],
 "is_residence": False,
 "is_dwelling": True,
 })
 elif use_parents_survival_cohabitation:
 step2_validation_info.append({
 "stat_name": stat_info.get("name", ""),
 "url": stat_info.get("url", ""),
 "columns": [col_survival, col_cohabitation],
 "is_residence": False,
 "is_parents_survival_cohabitation": True,
 })
 elif use_parents_expense_provider:
 step2_validation_info.append({
 "stat_name": stat_info.get("name", ""),
 "url": stat_info.get("url", ""),
 "columns": [col_expense],
 "is_residence": False,
 "is_parents_expense_provider": True,
 })
 elif use_housing_satisfaction:
 step2_validation_info.append({
 "stat_name": stat_info.get("name", ""),
 "url": stat_info.get("url", ""),
 "columns": [col_sat1, col_sat2, col_sat3],
 "is_residence": False,
 "is_housing_satisfaction": True,
 })
 elif use_spouse_economic:
 step2_validation_info.append({
 "stat_name": stat_info.get("name", ""),
 "url": stat_info.get("url", ""),
 "columns": [col_spouse],
 "is_residence": False,
 "is_spouse_economic": True,
 })
 elif use_employment_status:
 step2_validation_info.append({
 "stat_name": stat_info.get("name", ""),
 "url": stat_info.get("url", ""),
 "columns": [col_emp],
 "is_residence": False,
 "is_employment_status": True,
 })
 elif use_industry_major:
 step2_validation_info.append({
 "stat_name": stat_info.get("name", ""),
 "url": stat_info.get("url", ""),
 "columns": [col_ind],
 "is_residence": False,
 "is_industry_major": True,
 })
 elif use_job_class:
 step2_validation_info.append({
 "stat_name": stat_info.get("name", ""),
 "url": stat_info.get("url", ""),
 "columns": [col_job],
 "is_residence": False,
 "is_job_class": True,
 })
 elif use_work_satisfaction:
 step2_validation_info.append({
 "stat_name": stat_info.get("name", ""),
 "url": stat_info.get("url", ""),
 "columns": list(col_ws),
 "is_residence": False,
 "is_work_satisfaction": True,
 })
 elif use_pet_cost:
 step2_validation_info.append({
 "stat_name": stat_info.get("name", ""),
 "url": stat_info.get("url", ""),
 "columns": [col_pet_cost],
 "is_residence": False,
 "is_pet_cost": True,
 })
 elif use_income_consumption_satisfaction:
 step2_validation_info.append({
 "stat_name": stat_info.get("name", ""),
 "url": stat_info.get("url", ""),
 "columns": list(col_ics),
 "is_residence": False,
 "is_income_consumption_satisfaction": True,
 })
 elif use_education_cost:
 step2_validation_info.append({
 "stat_name": stat_info.get("name", ""),
 "url": stat_info.get("url", ""),
 "columns": list(col_edu),
 "is_residence": False,
 "is_education_cost": True,
 })
 elif use_other_region_consumption:
 step2_validation_info.append({
 "stat_name": stat_info.get("name", ""),
 "url": stat_info.get("url", ""),
 "columns": list(col_other),
 "is_residence": False,
 "is_other_region_consumption": True,
 })
 elif use_public_transport_satisfaction:
 step2_validation_info.append({
 "stat_name": stat_info.get("name", ""),
 "url": stat_info.get("url", ""),
 "columns": ["시내버스/마을버스 만족도", "시외/고속버스 만족도", "택시 만족도", "기타(기차,선박)만족도"],
 "is_residence": False,
 "is_preset": True,
 })
 elif use_medical_facility_main:
 step2_validation_info.append({"stat_name": stat_info.get("name", ""), "url": stat_info.get("url", ""), "columns": ["의료기관 주 이용시설"], "is_residence": False, "is_preset": True})
 elif use_medical_satisfaction:
 step2_validation_info.append({"stat_name": stat_info.get("name", ""), "url": stat_info.get("url", ""), "columns": ["의료시설 만족도"], "is_residence": False, "is_preset": True})
 elif use_welfare_satisfaction:
 step2_validation_info.append({"stat_name": stat_info.get("name", ""), "url": stat_info.get("url", ""), "columns": ["임신·출산·육아에 대한 복지 만족도", "저소득층 등 취약계층에 대한 복지 만족도"], "is_residence": False, "is_preset": True})
 elif use_provincial_satisfaction:
 step2_validation_info.append({"stat_name": stat_info.get("name", ""), "url": stat_info.get("url", ""), "columns": ["도정정책 만족도", "행정서비스 만족도"], "is_residence": False, "is_preset": True})
 elif use_social_communication:
 step2_validation_info.append({"stat_name": stat_info.get("name", ""), "url": stat_info.get("url", ""), "columns": ["사회적관계별 소통정도"], "is_residence": False, "is_preset": True})
 elif use_trust_people:
 step2_validation_info.append({"stat_name": stat_info.get("name", ""), "url": stat_info.get("url", ""), "columns": ["일반인에 대한 신뢰"], "is_residence": False, "is_preset": True})
 elif use_subjective_class:
 step2_validation_info.append({"stat_name": stat_info.get("name", ""), "url": stat_info.get("url", ""), "columns": ["주관적 귀속계층"], "is_residence": False, "is_preset": True})
 elif use_volunteer:
 step2_validation_info.append({"stat_name": stat_info.get("name", ""), "url": stat_info.get("url", ""), "columns": ["자원봉사 활동 여부", "자원봉사 활동 방식", "지난 1년 동안 자원봉사 활동 시간"], "is_residence": False, "is_preset": True})
 elif use_donation:
 step2_validation_info.append({"stat_name": stat_info.get("name", ""), "url": stat_info.get("url", ""), "columns": ["기부 여부", "기부 방식", "기부금액(만원)"], "is_residence": False, "is_preset": True})
 elif use_regional_belonging:
 step2_validation_info.append({"stat_name": stat_info.get("name", ""), "url": stat_info.get("url", ""), "columns": ["동네 소속감", "시군 소속감", "경상북도 소속감"], "is_residence": False, "is_preset": True})
 elif use_safety_eval:
 step2_validation_info.append({"stat_name": stat_info.get("name", ""), "url": stat_info.get("url", ""), "columns": ["(안전환경)어둡고 후미진 곳이 많다", "(안전환경)주변에 쓰레기가 아무렇게 버려져 있고 지저분 하다", "(안전환경)주변에 방치된 차나 빈 건물이 많다", "(안전환경)무리 지어 다니는 불량 청소년이 많다", "(안전환경)기초질서를 지키지 않는 사람이 많다", "(안전환경)큰소리로 다투거나 싸우는 사람들을 자주 볼 수 있다"], "is_residence": False, "is_preset": True})
 elif use_crime_fear:
 step2_validation_info.append({"stat_name": stat_info.get("name", ""), "url": stat_info.get("url", ""), "columns": ["(일상생활 범죄피해 두려움)나자신", "(일상생활 범죄피해 두려움)배우자(애인)", "(일상생활 범죄피해 두려움)자녀", "(일상생활 범죄피해 두려움)부모"], "is_residence": False, "is_preset": True})
 elif use_daily_fear:
 step2_validation_info.append({"stat_name": stat_info.get("name", ""), "url": stat_info.get("url", ""), "columns": ["(일상생활에서 두려움)밤에 혼자 집에 있을 때", "(일상생활에서 두려움)밤에 혼자 지역(동네)의 골목길을 걸을때"], "is_residence": False, "is_preset": True})
 elif use_law_abiding:
 step2_validation_info.append({"stat_name": stat_info.get("name", ""), "url": stat_info.get("url", ""), "columns": ["자신의 평소 준법수준", "평소 법을 지키지 않는 주된 이유"], "is_residence": False, "is_preset": True})
 elif use_environment_feel:
 step2_validation_info.append({"stat_name": stat_info.get("name", ""), "url": stat_info.get("url", ""), "columns": ["대기환경 체감도", "수질환경 체감도", "토양환경 체감도", "소음/진동환경 체감도", "녹지환경 체감도"], "is_residence": False, "is_preset": True})
 elif use_time_pressure:
 step2_validation_info.append({"stat_name": stat_info.get("name", ""), "url": stat_info.get("url", ""), "columns": ["평일 생활시간 압박", "주말 생활시간 압박"], "is_residence": False, "is_preset": True})
 elif use_leisure_satisfaction:
 step2_validation_info.append({"stat_name": stat_info.get("name", ""), "url": stat_info.get("url", ""), "columns": ["문화여가시설 만족도", "전반적인 여가활동 만족도", "여가활동 불만족 이유"], "is_residence": False, "is_preset": True})
 elif use_culture_attendance:
 step2_validation_info.append({"stat_name": stat_info.get("name", ""), "url": stat_info.get("url", ""), "columns": ["문화예술행사 관람 여부", "문화예술행사 관람 분야"], "is_residence": False, "is_preset": True})
 elif use_life_satisfaction:
 step2_validation_info.append({"stat_name": stat_info.get("name", ""), "url": stat_info.get("url", ""), "columns": ["삶에 대한 전반적 만족감(10점 만점)", "살고있는 지역의 전반적 만족감(10점 만점)", "어제 행복 정도(10점 만점)", "어제 걱정 정도(10점 만점)"], "is_residence": False, "is_preset": True})
 elif use_happiness_level:
 step2_validation_info.append({"stat_name": stat_info.get("name", ""), "url": stat_info.get("url", ""), "columns": ["생활수준(10점 만점)", "건강상태(10점 만점)", "성취도(10점 만점)", "대인관계(10점 만점)", "안전정도(10점 만점)", "지역사회소속감(10점 만점)", "미래안정성(10점 만점)"], "is_residence": False, "is_preset": True})
 elif use_residence_duration:
 step2_validation_info.append({
 "stat_name": stat_info.get("name", ""),
 "url": stat_info.get("url", ""),
 "columns": [col1, col2, col3],
 "is_residence": True,
 })
 else:
 _slug_col = f"kosis_{kosis._extract_category_code(stat_info.get('category', ''))}__{kosis._slug(stat_info.get('name', ''))}"
 step2_validation_info.append({
 "stat_name": stat_info.get("name", ""),
 "url": stat_info.get("url", ""),
 "columns": [_slug_col],
 "is_residence": False,
 })
 else:
 log_entry["status"] = "warning"
 log_entry["message"] = "대입 실패 (기본값 사용)"
 st.warning(f"[{stat_info['category']}] {stat_info['name']} 대입 실패 (기본값 사용)")
 except Exception as e:
 log_entry["status"] = "error"
 log_entry["message"] = str(e)
 log_entry["error"] = str(e)
 import traceback
 log_entry["traceback"] = traceback.format_exc()
 st.error(f"[{stat_info['category']}] {stat_info['name']} 대입 중 에러: {e}")
 
 st.session_state.stat_assignment_logs.append(log_entry)
 
 # 행 방향 논리 일관성 정리 (비경제활동 → 직장/직업/근로만족도 비움, 미성년 → 배우자 경제활동 무)
 result_df = apply_step2_row_consistency(result_df)
 
 # 개연성 적용 (캐시된 함수로 1회 계산 결과 재사용)
 result_df = apply_step2_logical_consistency_cached(result_df)
 display_state["step2_df"] = result_df
 display_state["generated_df"] = result_df
 display_state["show_step2_dialog"] = False
 step1_base_columns = ['식별NO', '가상이름', '거주지역', '성별', '연령', '경제활동', '교육정도', '월평균소득']
 added_columns = [col for col in result_df.columns if col not in step1_base_columns]
 display_state["step2_added_columns"] = added_columns
 display_state["step2_validation_info"] = step2_validation_info
 
 # ✅ 2단계 대입 완료 시 작업 기록에 추가된 통계 정보 저장
 from datetime import datetime
 if "work_logs" not in st.session_state:
 st.session_state.work_logs = []
 
 # 대입된 통계 정보 추출
 assigned_stats = []
 for stat_id in selected_stat_ids:
 stat_info = next((s for s in active_stats if s["id"] == stat_id), None)
 if stat_info:
 assigned_stats.append({
 "id": stat_info["id"],
 "category": stat_info["category"],
 "name": stat_info["name"],
 "url": stat_info.get("url", "")
 })
 
 step2_log = {
 "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
 "stage": "2단계 통계 대입 완료",
 "status": "success",
 "sido_code": sido_code,
 "sido_name": display_state.get("generated_sido_name", ""),
 "population_size": len(result_df),
 "assigned_statistics": assigned_stats,
 "added_columns": added_columns,
 "added_column_count": len(added_columns)
 }
 st.session_state.work_logs.append(step2_log)
 result_df_export = apply_step2_column_rename(result_df.copy())
 added_columns_export = [c for c in result_df_export.columns if c not in step1_base_columns]
 save_step2_record(result_df_export, sido_code, display_state.get("generated_sido_name", ""), added_columns_export)
 try:
 import io
 out_buffer = io.BytesIO()
 result_df_export.to_excel(out_buffer, index=False, engine="openpyxl")
 out_buffer.seek(0)
 display_state["generated_excel"] = out_buffer.getvalue()
 except Exception as e:
 st.warning(f"Excel 저장 실패: {e}")
 import traceback
 st.code(traceback.format_exc())
 st.success("2단계 완료: 통계 대입 완료.")
 st.rerun()
 except Exception as e:
 st.error(f"통계 대입 실패: {e}")
 import traceback
 st.code(traceback.format_exc())
 
 with col_cancel:
 if st.button("취소", use_container_width=True):
 display_state["show_step2_dialog"] = False
 st.rerun()
 
 st.markdown("---")
 # 결과 뷰용 세션 저장 (fragment 격리 실행 시 그래프/검증 탭에서 동일 데이터 사용)
 st.session_state["_rv_df"] = df
 st.session_state["_rv_is_step2"] = is_step2
 st.session_state["_rv_sido_name"] = sido_name
 st.session_state["_rv_n"] = n
 st.session_state["_rv_weights"] = weights or {}
 st.session_state["_rv_report"] = report or ""
 st.session_state["_rv_margins_axis"] = display_state.get("step1_margins_axis") or display_state.get("generated_margins_axis") or {}
 st.session_state["_rv_error_report"] = display_state.get("step1_error_report") or display_state.get("generated_error_report")
 st.session_state["_rv_step2_added_columns"] = display_state.get("step2_added_columns") or []
 st.session_state["_rv_step2_validation_info"] = display_state.get("step2_validation_info") or []
 st.session_state["_rv_generated_sido_code"] = display_state.get("generated_sido_code", "")
 fragment_result_tabs()


def page_step2_results():
 """2차 대입 결과: 날짜/시간별 기록 조회, 데이터 보기, 삭제(서버 파일까지 삭제). 여러 건 선택 후 일괄 삭제 가능. 페이지네이션(10건/페이지)."""
 from utils.step2_records import list_step2_records, delete_step2_record
 st.header("2차 대입 결과")
 records = list_step2_records()
 if not records:
 st.info("아직 2차 대입 결과가 없습니다. 가상인구 생성 후 2단계에서 통계를 대입하면 여기에 저장됩니다.")
 return
 # 페이지네이션: 한 페이지당 10건
 PER_PAGE = 10
 total_pages = max(1, (len(records) + PER_PAGE - 1) // PER_PAGE)
 if "step2_page" not in st.session_state:
 st.session_state["step2_page"] = 0
 current_page = min(max(0, st.session_state["step2_page"]), total_pages - 1)
 st.session_state["step2_page"] = current_page
 start = current_page * PER_PAGE
 end = min(start + PER_PAGE, len(records))
 page_records = records[start:end]

 st.caption(f"총 {len(records)}건 (날짜·시간순). 삭제 시 서버의 Excel·메타 파일이 함께 삭제됩니다.")
 st.markdown("**삭제할 항목을 체크한 뒤 아래 [선택한 항목 삭제] 버튼을 누르면 한 번에 삭제됩니다.**")
 for i, r in enumerate(page_records):
 idx = start + i
 ts = r.get("timestamp", "")
 sido_name = r.get("sido_name", "")
 rows = r.get("rows", 0)
 excel_path = r.get("excel_path", "")
 added = r.get("added_columns", [])
 row_label = f"{ts} | {sido_name} | {rows}명 | 추가 컬럼 {len(added)}개"
 with st.expander(row_label):
 st.checkbox("이 항목 삭제에 포함", key=f"step2_del_cb_{idx}")
 st.caption(f"추가된 컬럼: {', '.join(added[:8])}{' ...' if len(added) > 8 else ''}")
 # 지연 로딩: 단일 미리보기만 유지 (다른 미리보기 캐시 삭제 → 메모리 절약)
 preview_key = f"step2_show_preview_{idx}"
 df_cache_key = f"step2_preview_df_{idx}"
 if st.button("데이터 미리보기", key=f"step2_preview_btn_{idx}", type="secondary"):
 for k in list(st.session_state.keys()):
 if (k.startswith("step2_preview_df_") or k.startswith("step2_show_preview_") or k.startswith("step2_preview_excel_")) and k != df_cache_key and k != preview_key and k != f"step2_preview_excel_{idx}":
 del st.session_state[k]
 st.session_state[preview_key] = True
 st.rerun()
 # 미리보기를 펼쳤을 때만 파일 읽기·다운로드 버튼 렌더링 (루프마다 모든 파일 I/O 방지)
 if st.session_state.get(preview_key):
 if df_cache_key not in st.session_state:
 try:
 st.session_state[df_cache_key] = pd.read_excel(excel_path, engine="openpyxl")
 gc.collect()
 except Exception as e:
 st.warning(f"데이터 로드 실패: {e}")
 if df_cache_key in st.session_state:
 st.dataframe(
 st.session_state[df_cache_key].head(100),
 use_container_width=True,
 height=300,
 column_config={
 "페르소나": st.column_config.TextColumn("페르소나", width="large"),
 "현시대 반영": st.column_config.TextColumn("현시대 반영", width="large"),
 },
 )
 # 다운로드용 바이트는 캐시된 DataFrame에서 생성 (파일 재읽기 없음)
 excel_cache_key = f"step2_preview_excel_{idx}"
 if excel_cache_key not in st.session_state:
 try:
 buf = BytesIO()
 st.session_state[df_cache_key].to_excel(buf, index=False, engine="openpyxl")
 buf.seek(0)
 st.session_state[excel_cache_key] = buf.read()
 except Exception:
 st.session_state[excel_cache_key] = None
 col_dl, col_del = st.columns([1, 1])
 with col_dl:
 excel_bytes = st.session_state.get(excel_cache_key)
 if excel_bytes:
 st.download_button("Excel 다운로드", data=excel_bytes, file_name=os.path.basename(excel_path), mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key=f"dl_{ts}_{r.get('sido_code','')}_{idx}")
 with col_del:
 if st.button("이 항목만 삭제", key=f"del_step2_{ts}_{r.get('sido_code','')}_{idx}", type="secondary"):
 if delete_step2_record(excel_path):
 st.success("해당 2차 대입 결과와 서버 파일을 삭제했습니다.")
 else:
 st.error("삭제에 실패했습니다.")
 st.rerun()

 # 페이지 네비게이션
 if total_pages > 1:
 st.markdown("---")
 col_prev, col_info, col_next = st.columns([1, 2, 1])
 with col_prev:
 if st.button("← 이전", key="step2_prev_page", disabled=(current_page == 0)):
 st.session_state["step2_page"] = current_page - 1
 st.rerun()
 with col_info:
 st.caption(f"**{start + 1}–{end}** / {len(records)}건 (페이지 {current_page + 1}/{total_pages})")
 with col_next:
 if st.button("다음 →", key="step2_next_page", disabled=(current_page >= total_pages - 1)):
 st.session_state["step2_page"] = current_page + 1
 st.rerun()

 # 선택한 항목 일괄 삭제
 selected_paths = []
 for idx in range(len(records)):
 if st.session_state.get(f"step2_del_cb_{idx}", False):
 path = records[idx].get("excel_path", "")
 if path and path not in selected_paths:
 selected_paths.append(path)
 if selected_paths:
 if st.button("선택한 항목 삭제", type="primary", key="step2_bulk_delete"):
 success = 0
 fail = 0
 for path in selected_paths:
 if delete_step2_record(path):
 success += 1
 else:
 fail += 1
 if success:
 st.success(f"선택한 {success}건을 삭제했습니다." + (f" ({fail}건 실패)" if fail else ""))
 if fail:
 st.error(f"{fail}건 삭제에 실패했습니다.")
 st.rerun()
 else:
 st.caption("삭제할 항목을 위에서 체크하면 [선택한 항목 삭제] 버튼이 나타납니다.")




def page_step1_stat_log():
 """1차 생성(6축) 통계 대입 로그 페이지 - 1단계 KOSIS 6축 기반 생성 시 로그 확인. 현재 선택된 시도(URL ?sido= 또는 생성 탭 선택) 기준."""
 st.header("1차 통계 대입 로그")
 st.caption("1단계 가상인구 생성(6축 KOSIS 통계 대입) 시 기록된 로그입니다. **생성** 탭에서 가상인구 생성 실행 후 여기서 확인할 수 있습니다.")
 _sido = _get_current_gen_sido_code()
 _state = _get_generate_state(_sido)

 step1_log = _state.get("step1_debug_log") or ""
 if step1_log.strip():
 st.markdown("### 실행 로그 (1단계 생성 시)")
 st.text_area("로그 내용", value=step1_log, height=300, key="step1_debug_log_display", disabled=True, label_visibility="collapsed")
 else:
 st.info("실행 로그가 없습니다. **생성** 탭에서 시도 선택 후 **가상인구 생성** 버튼을 실행하면 1단계 로그가 여기에 기록됩니다.")

 st.markdown("---")
 st.markdown("### 교육정도(edu) 반영 진단")
 df_step1 = _state.get("step1_df")
 if df_step1 is None:
 df_step1 = _state.get("generated_df")
 margins_axis = _state.get("step1_margins_axis") or _state.get("generated_margins_axis") or {}
 edu_margin = margins_axis.get("edu") if margins_axis else None

 if df_step1 is not None and not df_step1.empty and "교육정도" in df_step1.columns:
 total_n = len(df_step1)
 actual_counts = df_step1["교육정도"].value_counts()
 actual_pct = {k: (v / total_n * 100) for k, v in actual_counts.items()}

 if edu_margin:
 labels_edu = edu_margin.get("labels", [])
 p_edu = edu_margin.get("p", edu_margin.get("probs", []))
 target_pct = {}
 for lb, p in zip(labels_edu, p_edu):
 target_pct[lb] = p * 100
 st.success("교육정도(edu): 1차 생성 시 **KOSIS 목표 분포가 반영**되었습니다. 아래에서 목표와 실제를 비교해 보세요.")
 all_labels = sorted(set(list(target_pct.keys()) + list(actual_pct.keys())))
 diag_data = []
 for lb in all_labels:
 t = target_pct.get(lb, 0)
 a = actual_pct.get(lb, 0)
 diff = a - t
 diag_data.append({
 "교육정도": lb,
 "목표 비율(%)": round(t, 2),
 "실제 비율(%)": round(a, 2),
 "오차(pp)": round(diff, 2),
 "비고": "목표 대비 부족" if diff < -1 else ("목표 대비 초과" if diff > 1 else "유사"),
 })
 st.dataframe(pd.DataFrame(diag_data), use_container_width=True, hide_index=True)
 if labels_edu:
 st.caption("목표 분포(1차 생성 시 사용): " + ", ".join(f"{lb}={target_pct.get(lb,0):.1f}%" for lb in labels_edu))
 else:
 st.warning("교육정도(edu): 1차 생성 시 **KOSIS 통계가 반영되지 않았습니다.** (기본 분포 25%/40%/35%로 생성됨 — CSV에는 교육정도 컬럼이 있으나 서울 KOSIS 비율이 아님)")
 edu_detail = _state.get("step1_edu_diagnostic_detail") or []
 if edu_detail:
 with st.expander("🔍 **이번 생성에서 edu 미반영 원인 (실제 진단 결과)**", expanded=True):
 for line in edu_detail:
 st.text(line)
 st.caption(
 "**원인 확인:** (1) **데이터 관리** 탭에서 **생성 시 선택한 시도와 같은 시도**를 선택했는지 확인하세요. "
 "(2) 해당 시도에서 **인구통계 기본 소스**의 **교육 (edu)** 에 KOSIS 교육 통계를 선택하고 **인구통계 기본 소스 저장**을 눌렀는지 확인하세요. "
 "(3) 저장 후 **생성** 탭에서 같은 시도를 선택한 뒤 가상인구를 다시 생성하세요. "
 "위 [실행 로그]에서 '교육정도(edu): KOSIS 반영됨'이 나와야 정상입니다."
 )
 diag_data = [{"교육정도": k, "실제 비율(%)": round(v, 2), "목표 비율(%)": "-", "오차(pp)": "-", "비고": "기본 분포로 생성됨"} for k, v in actual_pct.items()]
 st.dataframe(pd.DataFrame(diag_data), use_container_width=True, hide_index=True)
 else:
 st.info("교육정도 진단: 1단계 생성 데이터가 없거나 '교육정도' 컬럼이 없습니다. **생성** 탭에서 가상인구를 먼저 생성한 뒤 이 탭에서 확인하세요.")

 # 2) work_logs 중 1차(1단계) 관련만 필터
 work_logs = st.session_state.get("work_logs") or []
 step1_entries = [
 e for e in work_logs
 if isinstance(e, dict) and (
 (e.get("stage") or "").startswith("convert_kosis_to_distribution(")
 or (e.get("stage") == "1단계 가상인구 생성 완료")
 )
 ]
 if step1_entries:
 st.markdown("---")
 st.markdown("### 1차 통계 대입 기록 (6축·완료 요약)")
 # 최신순
 for idx, log in enumerate(reversed(step1_entries[-80:])):
 stage = log.get("stage", "N/A")
 ts = log.get("timestamp", "N/A")
 status = log.get("status", "unknown")
 if stage == "1단계 가상인구 생성 완료":
 with st.expander(f"✅ 1단계 완료 — {ts} | {log.get('sido_name', '')} {log.get('population_size', 0):,}명", expanded=(idx == 0)):
 st.json(log)
 else:
 axis_key = log.get("axis_key", "")
 err = log.get("error", "")
 with st.expander(f"{'✅' if status == 'success' else '❌'} {stage} — {ts} | label_count={log.get('label_count', 0)}", expanded=False):
 st.write(f"- **axis_key**: {axis_key}")
 st.write(f"- **status**: {status}")
 if err:
 st.write(f"- **error**: {err}")
 st.json(log)
 else:
 if not step1_log.strip():
 st.markdown("---")
 st.caption("6축 통계 변환(convert_kosis_to_distribution) 및 1단계 완료 기록도 생성 실행 후 여기에 표시됩니다.")


@st.fragment
def page_stat_assignment_log():
 """통계 대입 로그 페이지 - 2단계 통계 대입 시 발생하는 상세 로그 (fragment: 로그 영역만 갱신)"""
 st.header("통계 대입 로그")
 
 # 세션에서 통계 대입 로그 가져오기
 if "stat_assignment_logs" not in st.session_state:
 st.session_state.stat_assignment_logs = []
 
 if len(st.session_state.stat_assignment_logs) == 0:
 st.info("아직 통계 대입 로그가 없습니다. 2단계 통계 대입을 실행하면 로그가 기록됩니다.")
 return
 
 st.success(f"총 {len(st.session_state.stat_assignment_logs)}개의 통계 대입 로그")
 
 # 통계별로 그룹화
 stats_summary = {}
 for log in st.session_state.stat_assignment_logs:
 stat_key = f"{log.get('category', 'N/A')} - {log.get('stat_name', 'N/A')}"
 if stat_key not in stats_summary:
 stats_summary[stat_key] = {
 "total": 0,
 "success": 0,
 "warning": 0,
 "error": 0,
 "logs": []
 }
 stats_summary[stat_key]["total"] += 1
 stats_summary[stat_key]["logs"].append(log)
 status = log.get("status", "unknown")
 if status == "success":
 stats_summary[stat_key]["success"] += 1
 elif status == "warning":
 stats_summary[stat_key]["warning"] += 1
 elif status == "error":
 stats_summary[stat_key]["error"] += 1
 
 # 요약 표시
 st.markdown("### 통계별 요약")
 summary_data = []
 for stat_key, summary in stats_summary.items():
 summary_data.append({
 "통계명": stat_key,
 "총 시도": summary["total"],
 "성공": summary["success"],
 "경고": summary["warning"],
 "에러": summary["error"],
 "성공률": f"{(summary['success'] / summary['total'] * 100):.1f}%" if summary["total"] > 0 else "0%"
 })
 
 if summary_data:
 summary_df = pd.DataFrame(summary_data)
 st.dataframe(summary_df, use_container_width=True, hide_index=True)
 
 st.markdown("---")
 
 # 최신순으로 상세 로그 표시
 st.markdown("### 상세 로그 (최신순)")
 for idx, log in enumerate(reversed(st.session_state.stat_assignment_logs[-50:])):
 timestamp = log.get("timestamp", "N/A")
 category = log.get("category", "N/A")
 stat_name = log.get("stat_name", "N/A")
 status = log.get("status", "unknown")
 
 # 상태별 접두어 및 색상
 if status == "success":
 prefix = "[성공]"
 color = "green"
 elif status == "warning":
 prefix = "[경고]"
 color = "orange"
 elif status == "error":
 prefix = "[에러]"
 color = "red"
 else:
 prefix = "[대기]"
 color = "gray"
 
 with st.expander(f"{prefix} {timestamp} - [{category}] {stat_name}", expanded=False):
 col1, col2 = st.columns(2)
 with col1:
 st.markdown("**기본 정보**")
 st.write(f"- 카테고리: {category}")
 st.write(f"- 통계명: {stat_name}")
 st.write(f"- 상태: {status}")
 st.write(f"- 시각: {timestamp}")
 
 with col2:
 st.markdown("**데이터 정보**")
 kosis_data_count = log.get("kosis_data_count", 0)
 st.write(f"- KOSIS 데이터 건수: {kosis_data_count:,}건")
 url = log.get("url", "")
 if url:
 st.write(f"- URL: {url[:100]}..." if len(url) > 100 else f"- URL: {url}")
 
 # KOSIS 데이터 필드 정보
 kosis_data_fields = log.get("kosis_data_fields", [])
 if kosis_data_fields:
 st.write(f"- 데이터 필드: {', '.join(kosis_data_fields[:10])}" + ("..." if len(kosis_data_fields) > 10 else ""))
 
 # KOSIS 데이터 샘플 표시
 kosis_data_sample = log.get("kosis_data_sample", [])
 if kosis_data_sample:
 st.markdown("---")
 st.markdown("**가져온 KOSIS 데이터 샘플**")
 st.caption(f"전체 {kosis_data_count:,}건 중 처음 {len(kosis_data_sample)}건 표시")
 
 # 샘플 데이터를 DataFrame으로 변환하여 표시
 try:
 sample_df = pd.DataFrame(kosis_data_sample)
 st.dataframe(sample_df, use_container_width=True, hide_index=True)
 except Exception as e:
 # DataFrame 변환 실패 시 JSON으로 표시
 st.json(kosis_data_sample)
 
 message = log.get("message", "")
 if message:
 st.markdown("---")
 st.markdown(f"**메시지:** {message}")
 
 # 에러 정보 표시
 if status == "error":
 st.markdown("---")
 st.markdown("**에러 정보**")
 error = log.get("error", "")
 if error:
 st.error(f"에러: {error}")
 
 traceback_info = log.get("traceback", "")
 if traceback_info:
 with st.expander("상세 에러 추적", expanded=False):
 st.code(traceback_info, language="python")
 
 # 전체 로그 JSON (디버깅용)
 with st.expander("전체 로그 데이터 (JSON)", expanded=False):
 st.json(log)
 
 # 로그 초기화 버튼 (fragment 내부이므로 클릭 시 이 fragment만 갱신)
 col1, col2 = st.columns([1, 5])
 with col1:
 if st.button("로그 삭제", key="stat_assignment_log_delete"):
 st.session_state.stat_assignment_logs = []


def page_guide():
 st.header("사용 가이드")
 st.markdown("""
 ### 사용 순서
 1. **데이터 관리 탭**: 템플릿 업로드 및 6축 마진 소스 설정
 2. **생성 탭**: 생성 옵션 설정 후 생성 버튼 클릭
 3. **2차 대입 결과 탭**: 2단계 통계 대입 결과 기록 조회
 """)


# -----------------------------
# 8. Pages: 설문 진행 관련 함수들
# -----------------------------
# 공통 UI 컴포넌트: pages/common.py
# 설문조사 페이지: pages/survey.py
# 심층면접 페이지: pages/interview.py


def page_survey_form_builder():
 # Custom CSS 스타일링 (Indigo 테마)
 st.markdown("""
 
 /* 전체 배경 */
 .stApp {
 background-color: #FDFDFF;
 }
 
 /* 텍스트 영역 스타일 */
 .stTextArea > div > div > textarea {
 border-radius: 16px;
 border: 1px solid #e2e8f0;
 padding: 20px;
 font-size: 14px;
 transition: all 0.3s;
 }
 
 .stTextArea > div > div > textarea:focus {
 border-color: #4f46e5;
 box-shadow: 0 0 0 4px rgba(79, 70, 229, 0.1);
 outline: none;
 }
 
 /* 입력 필드 스타일 */
 .stTextInput > div > div > input {
 border-radius: 12px;
 border: 1px solid #e2e8f0;
 padding: 12px;
 font-size: 14px;
 }
 
 .stTextInput > div > div > input:focus {
 border-color: #4f46e5;
 outline: none;
 }
 
 /* 버튼 스타일 (그림자 제거) */
 .stButton > button {
 border-radius: 24px;
 font-weight: 800;
 font-size: 18px;
 padding: 20px 30px;
 transition: all 0.3s;
 box-shadow: none !important;
 }
 
 .stButton > button:hover {
 transform: translateY(-2px);
 box-shadow: none !important;
 }
 
 /* 카드 스타일 */
 .survey-card {
 background: white;
 border-radius: 24px;
 padding: 32px;
 border: 1px solid #e0e7ff;
 }
 
 .survey-card-indigo {
 background: #eef2ff;
 border: 1px solid #c7d2fe;
 }
 
 /* 배지 스타일 */
 .badge {
 display: inline-block;
 padding: 4px 12px;
 border-radius: 8px;
 font-size: 12px;
 font-weight: 700;
 }
 
 .badge-indigo {
 background: #eef2ff;
 color: #4f46e5;
 }
 
 /* 헤더 스타일 */
 .survey-header {
 margin-bottom: 24px;
 }
 
 """, unsafe_allow_html=True)
 
 # 세션 상태 초기화
 if "survey_definition" not in st.session_state:
 st.session_state.survey_definition = ""
 if "survey_needs" not in st.session_state:
 st.session_state.survey_needs = ""
 if "survey_target" not in st.session_state:
 st.session_state.survey_target = ""
 if "survey_website" not in st.session_state:
 st.session_state.survey_website = ""
 if "survey_custom_mode" not in st.session_state:
 st.session_state.survey_custom_mode = False
 
 # 헤더 제거됨 (설문조사/심층면접 페이지는 pages/survey.py, pages/interview.py에서 처리)
 
 # 메인 타이틀
 st.markdown("""
 
 
 새로운 시장성 조사 설계 시작하기
 
 
 제품과 니즈를 상세히 적어주실수록, AI 가상패널이 더 정교한 인사이트를 도출합니다.
 
 
 """, unsafe_allow_html=True)
 
 # 2단 분할 레이아웃
 left_col, right_col = st.columns([0.65, 0.35], gap="large")
 
 with left_col:
 # 필수 입력 1: 제품 정의
 st.markdown("### 제품/서비스의 정의 * ", unsafe_allow_html=True)
 
 definition_length = len(st.session_state.survey_definition)
 is_definition_valid = definition_length >= 300
 
 col_def_label, col_def_count = st.columns([3, 1])
 with col_def_count:
 if is_definition_valid:
 st.markdown(f" {definition_length} / 300자 이상 ", unsafe_allow_html=True)
 else:
 st.markdown(f" {definition_length} / 300자 이상 ", unsafe_allow_html=True)
 
 definition = st.text_area(
 "제품/서비스 정의",
 value=st.session_state.survey_definition,
 placeholder="제품의 핵심 기능, 가치, 시장 내 위치 등을 상세히 작성해주세요.",
 height=200,
 key="survey_definition_input",
 label_visibility="collapsed"
 )
 st.session_state.survey_definition = definition
 
 st.markdown(" ", unsafe_allow_html=True)
 
 # 필수 입력 2: 조사의 니즈
 st.markdown("### 조사의 목적과 니즈 * ", unsafe_allow_html=True)
 
 needs_length = len(st.session_state.survey_needs)
 is_needs_valid = needs_length >= 300
 
 col_needs_label, col_needs_count = st.columns([3, 1])
 with col_needs_count:
 if is_needs_valid:
 st.markdown(f" {needs_length} / 300자 이상 ", unsafe_allow_html=True)
 else:
 st.markdown(f" {needs_length} / 300자 이상 ", unsafe_allow_html=True)
 
 needs = st.text_area(
 "조사의 목적과 니즈",
 value=st.session_state.survey_needs,
 placeholder="이번 조사를 통해 무엇을 알고 싶으신가요? (예: 타겟 유저의 가격 저항선, 경쟁사 대비 강점 등)",
 height=200,
 key="survey_needs_input",
 label_visibility="collapsed"
 )
 st.session_state.survey_needs = needs
 
 st.markdown(" ", unsafe_allow_html=True)
 
 # 선택 입력 섹션
 with st.expander("추가 정보 (선택)", expanded=False):
 col_target, col_website = st.columns(2)
 with col_target:
 st.text_input(
 "희망 타깃",
 value=st.session_state.survey_target,
 placeholder="특정 타깃이 있다면 적어주세요. (예: 30대 워킹맘)",
 key="survey_target_input"
 )
 with col_website:
 st.text_input(
 "홈페이지 주소",
 value=st.session_state.survey_website,
 placeholder="https://",
 key="survey_website_input"
 )
 
 st.file_uploader(
 "참고자료 업로드",
 type=["pdf", "jpg", "jpeg", "png", "ppt", "pptx"],
 key="survey_file_upload",
 help="PDF, JPG, PPT 형식 지원"
 )
 
 with right_col:
 # AI 최적화 설계 제안 카드
 is_all_valid = is_definition_valid and is_needs_valid
 
 if is_all_valid:
 card_class = "survey-card-indigo"
 else:
 card_class = "survey-card"
 
 st.markdown(f' ', unsafe_allow_html=True)
 
 st.markdown("""
 
 AI 최적화 설계 제안 
 
 """, unsafe_allow_html=True)
 
 if not is_all_valid:
 st.warning("필수 정보를 300자 이상 입력하시면 AI가 최적의 조사 설계를 제안합니다.")
 else:
 # 권장 조사 방식
 col_rec1, col_rec2 = st.columns([1, 1])
 with col_rec1:
 st.markdown("**권장 조사 방식**")
 with col_rec2:
 st.markdown(' 질적 조사 (Talk) ', unsafe_allow_html=True)
 
 st.markdown(" ", unsafe_allow_html=True)
 
 # 최적 페르소나 그룹
 col_persona1, col_persona2 = st.columns([1, 1])
 with col_persona1:
 st.markdown("**최적 페르소나 그룹**")
 with col_persona2:
 st.markdown("**2,500명 (다변량 추출)**")
 
 st.markdown(" ", unsafe_allow_html=True)
 
 # AI 코멘트
 st.info("""
 **AI 코멘트**
 
 입력하신 니즈를 분석한 결과, 구체적인 구매 방해 요소를 파악하기 위해 
 **수천 명의 가상 패널과의 심층 토론(Talk)**이 가장 효과적일 것으로 예측됩니다.
 """)
 
 st.markdown(" ", unsafe_allow_html=True)
 
 # 커스텀 모드 토글
 col_toggle1, col_toggle2 = st.columns([2, 1])
 with col_toggle1:
 st.markdown(" 조사 구체 계획이 있으신가요? ", unsafe_allow_html=True)
 with col_toggle2:
 custom_mode = st.toggle(
 "맞춤형(Custom) 모드",
 value=st.session_state.survey_custom_mode,
 key="survey_custom_toggle"
 )
 st.session_state.survey_custom_mode = custom_mode
 
 if custom_mode:
 st.markdown("---")
 st.markdown("**조사 방식 변경**")
 survey_type = st.radio(
 "조사 방식",
 options=["Talk", "Survey"],
 key="survey_type_radio",
 horizontal=True
 )
 st.session_state.survey_type = survey_type
 
 st.markdown(" ", unsafe_allow_html=True)
 st.markdown("**표본 수 조정**")
 sample_size = st.slider(
 "표본 수",
 min_value=100,
 max_value=10000,
 value=2500,
 step=100,
 key="survey_sample_slider"
 )
 st.session_state.survey_sample_size = sample_size
 st.caption(f"최소 100명 ~ 최대 10,000명 (현재: {sample_size:,}명)")
 else:
 st.session_state.survey_type = "Talk"
 st.session_state.survey_sample_size = 2500
 
 st.markdown(' ', unsafe_allow_html=True)
 
 st.markdown(" ", unsafe_allow_html=True)
 
 # 조사 시작하기 버튼
 if is_all_valid:
 if st.button("조사 시작하기", type="primary", use_container_width=True, key="survey_start_button"):
 survey_type = st.session_state.get("survey_type", "Talk")
 sample_size = st.session_state.get("survey_sample_size", 2500)
 target = st.session_state.survey_target if st.session_state.survey_target else "전체"
 
 st.success("조사 프로젝트가 시작되었습니다.")
 st.info(f"""
 **설정된 조사 정보:**
 - 조사 방식: {survey_type}
 - 표본 수: {sample_size:,}명
 - 타깃: {target}
 """)
 else:
 st.button("조사 시작하기", disabled=True, use_container_width=True, key="survey_start_button_disabled")
 st.caption("필수 정보를 300자 이상 입력해주세요.")
 
 st.markdown(" ", unsafe_allow_html=True)
 
 # 정보 안내
 st.caption("데이터는 암호화되어 보호되며, 분석 완료 후 즉시 파기됩니다.")


def page_survey_results():
 st.subheader("설문 결과")
 st.info("설문 결과 기능은 추후 확장 범위")


# -----------------------------
# 9. Main UI (상위 폴더 구조)
# -----------------------------
def render_landing():
 """랜딩 페이지: 로고 이미지 + 시작하기 버튼, 사이드바 숨김"""
 st.markdown("""
 
 [data-testid="stSidebar"] { display: none; }
 header[data-testid="stHeader"] { display: none; }
 
 """, unsafe_allow_html=True)
 logo_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "assets", "logo.png")
 if os.path.isfile(logo_path):
 col1, col2, col3 = st.columns([1, 2, 1])
 with col2:
 st.image(logo_path, use_container_width=True)
 else:
 st.markdown(' Social Simulation ', unsafe_allow_html=True)
 st.markdown(" ", unsafe_allow_html=True)
 col1, col2, col3 = st.columns([1, 1, 1])
 with col2:
 if st.button("시작하기", type="primary", use_container_width=True, key="landing_start"):
 st.session_state.app_started = True
 st.rerun()


def _ensure_generate_modules() -> None:
 """가상인구 생성 탭 전용 무거운 모듈 로드 (해당 탭 진입 시에만 실행)."""
 if "KosisClient" in globals() and globals().get("KosisClient") is not None:
 return
 try:
 from google import genai
 from utils.kosis_client import KosisClient
 from utils.ipf_generator import generate_base_population
 from utils.gemini_client import GeminiClient
 from utils.step2_records import STEP2_RECORDS_DIR, list_step2_records, save_step2_record
 globals()["genai"] = genai
 globals()["KosisClient"] = KosisClient
 globals()["generate_base_population"] = generate_base_population
 globals()["GeminiClient"] = GeminiClient
 globals()["STEP2_RECORDS_DIR"] = STEP2_RECORDS_DIR
 globals()["list_step2_records"] = list_step2_records
 globals()["save_step2_record"] = save_step2_record
 except Exception:
 raise


def _run_page_vdb():
 from pages.virtual_population_db import page_virtual_population_db
 st.title(APP_TITLE)
 page_virtual_population_db()


def _run_page_generate():
 from pages.generate import run_generate
 run_generate()


def _run_page_survey():
 from pages.survey import page_survey
 st.title(APP_TITLE)
 page_survey()


def _run_page_conjoint():
 from pages.result_analysis_conjoint import page_conjoint_analysis
 st.title(APP_TITLE)
 page_conjoint_analysis()


def _run_page_psm():
 from pages.result_analysis_psm import page_psm
 st.title(APP_TITLE)
 page_psm()


def _run_page_bass():
 from pages.result_analysis_bass import page_bass
 st.title(APP_TITLE)
 page_bass()


def _run_page_statcheck():
 from pages.result_analysis_statcheck import page_statcheck
 st.title(APP_TITLE)
 page_statcheck()


def _run_page_stats_preprocess():
 st.title(APP_TITLE)
 page_stats_preprocess()


def main():
 # set_page_config는 run.py에서 이미 1회 호출됨. 여기서 다시 호출하면 Streamlit Cloud 등에서 "can only be called once" 오류로 로딩 실패할 수 있음.
 # st.set_page_config(page_title=APP_TITLE, layout="wide")
 
 # 페이지 전환 시 이전 콘텐츠 잔상(ghosting) 방지 (st.navigation 메뉴는 사이드바에 그대로 표시)
 st.markdown("""
 
 [data-testid="stAppViewContainer"] main .block-container { opacity: 1 !important; }
 
 """, unsafe_allow_html=True)

 # DB 초기화: Supabase 연결 검증 (세션당 1회 성공 시만 플래그 설정)
 if not st.session_state.get("_db_initialized", False):
 with st.spinner("준비 중…"):
 try:
 db_init()
 st.session_state.pop("db_init_error", None)
 st.session_state["_db_initialized"] = True
 except Exception as e:
 st.session_state["db_init_error"] = str(e)
 ensure_session_state()

 # 엄격한 단일 컨테이너: 모든 메인 UI는 이 플레이스홀더 안에서만 렌더 (잔상 방지)
 if "_main_placeholder" not in st.session_state:
 st.session_state["_main_placeholder"] = st.empty()
 main_container = st.session_state["_main_placeholder"]

 # URL에 ?sido= 또는 ?page=generate 가 있으면 랜딩 건너뛰고 바로 메인으로 (저장 후 새로고침 시 복원)
 _qp = st.query_params
 if not st.session_state.get("app_started", False) and (_qp.get("sido") or _qp.get("page") == "generate"):
 st.session_state["app_started"] = True

 if not st.session_state.get("app_started", False):
 main_container.empty()
 with main_container.container():
 render_landing()
 if st.session_state.get("db_init_error"):
 st.error("Supabase 설정을 확인해주세요. " + st.session_state["db_init_error"])
 return

 # 사이드바: 메모리 정리 (캐시·GC로 장시간 실행 시 메모리 절약)
 with st.sidebar:
 st.caption("시스템")
 if st.button("메모리 정리", key="mem_clear_btn", type="secondary", use_container_width=True):
 try:
 st.cache_data.clear()
 except Exception:
 pass
 gc.collect()
 st.success("캐시 및 메모리 정리를 실행했습니다.")
 st.rerun()

 # 페이지 전환 시 컨테이너는 각 _run_page_* 내부에서 empty() 후 채움
 # st.navigation: 페이지 전환 시 st.rerun() 없이 전환되어 깜빡임·지연 최소화
 # URL에 ?page=generate 가 있으면 새로고침 시 가상인구 생성 페이지로 진입
 _want_generate = st.query_params.get("page") == "generate"
 page_vdb = st.Page(_run_page_vdb, title="가상인구 DB", default=not _want_generate)
 page_gen = st.Page(_run_page_generate, title="가상인구 생성", default=_want_generate)
 page_survey = st.Page(_run_page_survey, title="시장성 조사 설계")
 page_conjoint = st.Page(_run_page_conjoint, title="[선호도 분석]컨조인트 분석")
 page_psm = st.Page(_run_page_psm, title="[가격 수용성]PSM")
 page_bass = st.Page(_run_page_bass, title="[시장 확산 예측]Bass 확산 모델")
 page_statcheck = st.Page(_run_page_statcheck, title="[가설 검증]A/B 테스트 검증")
 page_stats_preprocess = st.Page(_run_page_stats_preprocess, title="통계 전처리")

 nav = st.navigation({
 "AI Social Twin": [page_vdb, page_gen, page_survey],
 "Result analysis": [page_conjoint, page_psm, page_bass, page_statcheck],
 "Utils": [page_stats_preprocess],
 })
 nav.run()


if __name__ == "__main__":
 import streamlit as _st
 _st.set_page_config(page_title=APP_TITLE, layout="wide")
 try:
 main()
 except Exception as e:
 import streamlit as _st
 _st.error("앱 로드 중 오류가 발생했습니다.")
 _st.code(str(e))
 import traceback
 _st.code(traceback.format_exc())
