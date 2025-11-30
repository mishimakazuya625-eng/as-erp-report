# AS ERP System

Streamlit 기반의 제조업 ERP 시스템입니다. 재고, BOM, 주문, 결품 분석 등을 관리할 수 있습니다.

## 주요 기능

- **상품 관리 (Product Master)**: 제품 등록, 수정, 삭제
- **BOM & 대체자재**: 자재 명세서 관리 및 대체 부품 추천
- **주문 관리 (Order Management)**: PO 업로드, UPSERT 로직, 주문 추적
- **재고 관리**: Plant Site 관리, 스냅샷 기반 재고 이력
- **결품 분석 리포트**: 
  - Pre-Filtering (고객사, 생산처, 주문 상태)
  - R1: 고객사-생산처별 통합 현황
  - R2: PKID별 Wide Format 결품 상세 (URGENT 플래그, 대체품 재고)

## 기술 스택

- **프론트엔드**: Streamlit
- **백엔드**: Python 3.11+
- **데이터베이스**: PostgreSQL (Supabase)
- **데이터 처리**: Pandas

## 로컬 설치 및 실행

### 1. 저장소 클론
```bash
git clone https://github.com/YOUR_USERNAME/YOUR_REPO.git
cd YOUR_REPO
```

### 2. 라이브러리 설치
```bash
pip install -r requirements.txt
```

### 3. 데이터베이스 설정

#### 방법 A: Supabase (클라우드)
1. [Supabase](https://supabase.com/)에서 무료 프로젝트 생성
2. Connection String 복사
3. `.streamlit/secrets.toml` 파일 생성:
   ```bash
   cp .streamlit/secrets.toml.example .streamlit/secrets.toml
   ```
4. `secrets.toml`에 Connection String 입력

#### 방법 B: 로컬 PostgreSQL
1. PostgreSQL 설치
2. 데이터베이스 생성:
   ```sql
   CREATE DATABASE as_erp;
   ```
3. `.streamlit/secrets.toml` 생성:
   ```toml
   db_url = "postgresql://postgres:비밀번호@localhost:5432/as_erp"
   ```

### 4. 앱 실행
```bash
streamlit run main.py
```

브라우저에서 `http://localhost:8501` 접속

## Streamlit Cloud 배포

### 1. GitHub에 푸시
```bash
git add .
git commit -m "Initial commit"
git push origin main
```

### 2. Streamlit Cloud 배포
1. [Streamlit Cloud](https://share.streamlit.io/)에 로그인
2. "New app" 클릭
3. GitHub 저장소, 브랜치(main), 파일(main.py) 선택
4. **Advanced settings** > **Secrets** 에 다음 입력:
   ```toml
   db_url = "YOUR_SUPABASE_CONNECTION_STRING"
   ```
5. "Deploy!" 클릭

## 주의사항

⚠️ **보안**
- `.streamlit/secrets.toml`은 **절대 Git에 커밋하지 마세요** (`.gitignore`에 포함됨)
- GitHub에 올릴 때는 비밀번호를 제거하세요
- Streamlit Cloud 배포 시 Secrets 메뉴에서 설정하세요

## 데이터베이스 스키마

앱을 처음 실행하면 자동으로 다음 테이블이 생성됩니다:
- `Product_Master`: 제품 정보
- `BOM_Master`: 자재 명세서
- `Substitute_Master`: 대체 부품
- `AS_Order`: 주문 정보
- `Plant_Site_Master`: 생산 사이트
- `Inventory_Master`: 재고 스냅샷 (복합키: PKID, PLANT_SITE, SNAPSHOT_DATE)

## 라이선스

MIT License

## 문의

이슈가 있으면 GitHub Issues에 등록해주세요.
