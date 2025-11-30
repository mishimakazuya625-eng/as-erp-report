# GitHub 배포 가이드

## 📦 파일 구조
```
aserp2/
├── main.py                          # 메인 애플리케이션
├── bom_substitute_master.py         # BOM 관리
├── order_management.py              # 주문 관리
├── schema_update_module.py          # 재고/사이트 관리
├── shortage_analysis_report.py      # 결품 분석
├── requirements.txt                 # Python 패키지
├── .gitignore                       # Git 제외 파일
├── .streamlit/
│   └── secrets.toml.example        # 비밀 설정 예제
└── README.md                        # 프로젝트 설명
```

## 🚀 GitHub 업로드 단계

### 1. Git 초기화
```bash
cd C:\Users\ejsej\.gemini\antigravity\scratch\stock_app\aserp2
git init
git add .
git commit -m "Initial commit: AS ERP System"
```

### 2. GitHub 저장소 생성
1. GitHub.com에 로그인
2. 우측 상단 **+** → **New repository**
3. 저장소 이름 입력 (예: `as-erp-system`)
4. **Public** 또는 **Private** 선택
5. **Create repository** 클릭

### 3. 원격 저장소 연결 및 푸시
```bash
git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPO_NAME.git
git branch -M main
git push -u origin main
```

## ☁️ Streamlit Cloud 배포

### 1. Streamlit Cloud 접속
- https://share.streamlit.io/ 에서 GitHub 계정으로 로그인

### 2. 앱 배포
1. **New app** 클릭
2. 설정:
   - **Repository**: 방금 만든 저장소 선택
   - **Branch**: `main`
   - **Main file path**: `main.py`

### 3. Secrets 설정 (중요!)
1. **Advanced settings** 클릭
2. **Secrets** 탭 선택
3. 다음 내용 입력:
   ```toml
   db_url = "postgresql://postgres:[비밀번호]@db.[프로젝트ID].supabase.co:5432/postgres"
   ```
4. **Deploy!** 클릭

### 4. 배포 완료
- 몇 분 후 앱이 활성화됩니다
- URL: `https://your-app-name.streamlit.app`

## 🔒 보안 체크리스트

- ✅ `.gitignore`에 `secrets.toml` 포함됨
- ✅ `secrets.toml.example`만 업로드됨 (실제 비밀번호 없음)
- ✅ Streamlit Cloud Secrets에 DB 정보 입력
- ⚠️ **절대** 실제 비밀번호를 GitHub에 업로드하지 마세요!

## 📝 Git 명령어 참고

```bash
# 상태 확인
git status

# 변경사항 추가
git add .

# 커밋
git commit -m "메시지"

# 푸시
git push origin main

# 변경 이력 보기
git log
```

## 🆘 문제 해결

### "secrets.toml not found" 오류
→ Streamlit Cloud > Settings > Secrets에서 `db_url` 확인

### "Database connection failed" 오류
→ Supabase 프로젝트 활성 상태 및 Connection String 확인

### Git push 거부됨
→ `git pull origin main` 먼저 실행 후 다시 push
