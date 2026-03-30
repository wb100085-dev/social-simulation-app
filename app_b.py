"""
상업용 페이지 엔트리포인트.
- 관리자용 app.py는 건드리지 않고 유지
- 시장성 조사 설계 페이지만 단독 제공
- 설계 로직은 pages/survey.py를 그대로 재사용하여 관리자용 변경사항 자동 연동
"""

import streamlit as st

from pages.survey import page_survey


APP_B_TITLE = "AI Social Twin - 시장성 조사 설계"


def main() -> None:
    st.markdown(
        """
        <style>
        [data-testid="stSidebar"] { display: none; }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.title(APP_B_TITLE)
    page_survey()


if __name__ == "__main__":
    st.set_page_config(page_title=APP_B_TITLE, layout="wide")
    main()
