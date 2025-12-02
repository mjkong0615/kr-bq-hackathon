# [KR] BigQuery Hackathon


## **Overview**


본 챌린지 랩은 Google Cloud의 데이터 분석 솔루션(BQML, Gemini in Bigquery 등)과 머신 러닝 솔루션을 활용하여, 고객 데이터 분석부터 실행까지 이어지는 엔드투엔드(end-to-end) 개인화 마케팅 파이프라인을 구축하는 실습 형태의 해커톤입니다.

해커톤은 팀 단위로 운영하여 총 4명이 한 팀이 됩니다: 관리자가 3명의 팀원들에게 사용자 계정을 부여하여 총 4명의 팀원이 작업을 수행합니다.

## Objective

* **데이터 분석 (Analysis):** BigQuery와 Gemini를 함께 사용하여 이미지, 비디오, 텍스트 등 멀티모달(multimodal) 고객 리뷰의 감성(Sentiment)을 분석합니다.
* **세분화 및 타겟팅 (Segment & Target):** EDA를 통해 고객을 세분화(Segmentation)하고, 특히 부정적인 피드백을 남긴 고객을 식별하여 이들을 위한 맞춤형 프로모션 메시지를 생성합니다.
* **모델링 및 예측 (Model & Predict):** 고도화된 제품 추천 모델을 만들기 위해 추가 EDA를 진행하고 피처(feature)를 도출한 뒤, BigQuery Studio의 Data Science Agent를 활용해 고급 추천 모델을 구축하고 예측 결과를 BigQuery 테이블로 저장합니다.
* **자동화 및 실행 (Automate & Activate):** BigQuery의 고객 경험 기반의 데이터 분석 결과를 Application Integration을 통해 맞춤형 이메일을 자동으로 전송하는 워크플로우를 구축하여 확보된 인사이트를 즉각적인 마케팅 활동으로 연결합니다.

## Scenario

온라인 리테일 플랫폼을 운영하는 Cymbal E-Commerce는 온라인 플랫폼을 강화하여 변화하는 고객 니즈에 빠르게 대응하고자 합니다. 이를 달성하기 위해 고객의 데이터를 분석하고, BigQuery와 Gemini를 활용해서 고객 이탈률을 최소화하는 방안을 구축하고 있습니다.

이러한 계획은 고객 피드백과 선호도를 신속하게 분석해야 할 필요성에 따라 추진되었습니다. Cymbal E-Commerce는 이 분석 결과를 바탕으로 부정적인 피드백을 남긴 고객에게 맞춤형 프로모션 전략을 적용하고, 변화하는 고객 니즈에 기민하게 대처할 수 있습니다. 또한, 감성 분석 기반의 개인화된 제품 추천을 제공하여 고객의 온라인 쇼핑 경험을 한층 매력적으로 만들고자 합니다.

Cymbal E-Commerce는 이러한 혁신을 이루는 데 생성형 AI(Generative AI)가 핵심적인 역할을 할 것이라 판단하고 Google Cloud를 선택했습니다. Google Cloud의 최첨단 AI 기술은 이러한 지능형 솔루션을 개발하고 확장하는 데 필요한 강력한 토대를 제공함으로써, 리테일 업계에서 Cymbal E-Commerce의 선도적인 위치를 확고히 다져줄 것입니다.

## Your Challenge

이 실습에서는 Cymbal E-Commerce의 전략적 목표를 신속하게 구현합니다. 여러분은 BigQuery를 활용하여 고객 피드백을 빠르게 분석하고, 이 결과를 바탕으로 맞춤형 캠페인 전략을 수립하고 구체화하게 됩니다. 더 나아가, BigQuery의 여러 기능을 활용하여 각 채널에 최적화된 매력적인 콘텐츠를 제작하고, 이를 다양한 마케팅 채널에서 실행하는 과정을 경험합니다.

캠페인 실행 외에도, 여러분은 제품 카탈로그 관리 기능을 개선하고 고객 이탈률(Churn Rate) 최소화를 위한 추천 모델을 고도화해야 합니다. 무엇보다, BigQuery ML(BQML)을 기반으로 데이터를 정확하게 분석하고, 모델의 성능을 테스트 및 검증하는 것이 중요합니다.
특히, BQML을 사용하면 마케팅 전략의 수립, 실행, 테스트 프로세스 속도를 획기적으로 단축할 수 있습니다.

## Task Outline

* Task 1: 마케팅 인사이트를 위한 고객 리뷰 멀티모달 분석
* Task 2: 고객 세분화를 통한 타겟 마케팅  
* Task 3: 불만족 고객을 위한 맞춤형 프로모션 상품 추천 및 평가
* Task 4: 추가적인 탐색적 데이터 분석(EDA)
* Task 5: 상품 추천 모델 생성 및 활용
* Task 6: 멀티 소스 데이터를 활용한 고객 리타겟팅

## Task Dependencies

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/2bdc11dcabc95304.png" alt="2bdc11dcabc95304.png"  width="624.00" />



## Task 1: 마케팅 인사이트를 위한 고객 리뷰 멀티모달 분석


#### **Overview**

고객 리뷰 분석 실습에 오신 것을 환영합니다! 본 태스크에서는 텍스트 뿐 아니라 이미지와 동영상을 포함하는 멀티모달 고객 리뷰 데이터를 다룹니다. 목표는 BigQuery와 Vertex AI의 Gemini Pro 모델이 제공하는 강력한 기능을 활용하여 이 다양한 형태의 데이터를 처리하는 것입니다. 분석 결과를 통합하여 고객 경험에 대한 깊이 있는 인사이트를 도출하고, 이를 비즈니스에 실질적으로 적용하는 방법을 학습합니다.

#### **Objective**

* BigQuery Studio에서 챌린지 실습 Notebook을 설정하고 실행합니다.
* 외부 테이블을 생성하여 Cloud Storage에 저장된 파일의 데이터를 직접 쿼리합니다.
* Gemini 모델을 호출하여 텍스트, 이미지, 비디오 리뷰의 콘텐츠를 분석합니다.
* 모든 분석 결과를 하나의 통합 테이블로 결합합니다.
* 감성 분포와 시간 경과에 따른 트렌드를 시각화하여 실행 가능한 비즈니스 인사이트로 도출합니다.

#### **Setup**

이 초기 환경 설정 단계에서는 BigQuery와 Cloud Storage가 Vertex AI Gemini 모델과 원활하게 데이터를 주고받을 수 있도록 필요한 권한을 구성합니다.

#### **시작하기 전: Cloud Storage의 샘플 데이터 탐색**

실습 단계를 시작하기 전에 작업할 샘플 데이터를 간단히 살펴봅니다. Cloud Storage Bucket에서 고객 리뷰 텍스트, 이미지, 비디오에 직접 액세스합니다. 이를 통해 멀티모달 데이터를 이해합니다.

1. Google Cloud 콘솔에서 **Navigation Menu**(☰)로 이동하여 **Cloud Storage &gt; Buckets**를 선택합니다.
2. 실습 환경에 제공된 Bucket 이름을 클릭합니다 ({your-project-id}-bucket, 예: qwiklabs-gcp-xx-xxxxx-bucket).
3. **Buckets** 안에서 review/ 폴더로 이동합니다.
4. 해당 폴더에서는 다음 항목을 확인합니다:

* **고객 리뷰 (CSV):** customer_reviews.csv를 클릭하여 원시 텍스트 리뷰 데이터를 미리 봅니다.
* **리뷰 이미지:** images/ 폴더로 들어가 샘플 이미지 파일을 확인합니다. 각 이미지 > Public URL/Authenticated URL을 클릭하여 조회합니다.
* **리뷰 비디오:** videos/ 폴더로 들어가 샘플 비디오 파일을 확인합니다. 각 비디오 > Public URL/Authenticated URL을 클릭하여 콘솔에서 재생합니다.

본격적인 분석 작업을 진행하기 전에 이 파일들을 자유롭게 탐색하여 데이터 내용에 충분히 익숙해집니다.

### 1. BigQuery 환경 설정

### **1.1 BigQuery Cloud 리소스 연결 생성**

먼저, BigQuery가 Gemini 모델과 작동할 수 있도록 Cloud 리소스 연결을 생성합니다.

BigQuery 내부에서 SQL 쿼리만으로 외부의 Gemini 모델을 직접 호출하기 위해서는 상호 작용을 위한 인증 정보와 권한이 포함된 '연결(Connection)' 리소스가 필요합니다. 이 단계에서는 그 연결 고리를 생성합니다.

1. Google Cloud 콘솔에서 **Navigation menu**(☰)로 이동하여 **BigQuery**를 선택합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task1_explorer.png" alt="task1_explorer.png"  width="348.90" />

2. **Explorer** 패널에서 **+ Add Data**를 클릭한 다음, Vertex AI를 입력하고 Vertex AI를 클릭 한 뒤 나오는 **BigQuery Federation**을 클릭합니다.
3. **connection ID**에 **gemini_conn**을 입력합니다.
4. **리전 유형**으로 **us-central1**을 선택한 다음, 드롭다운에서 **us-central1**을 선택합니다.

> 이번 핸즈온 실습에서 us-central1을 제약적으로 사용하고 있습니다.

5. **CREATE CONNECTION**을 클릭합니다.
6. 확인 창이 나타납니다. **Go to connection**을 클릭합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task1_gotoconnection.png" alt="gotoconnection.png"  width="548.90" />

7. **Connection info** 창(us-central1.gemini_conn)에서 **Service account ID**를 찾아 텍스트 편집기에 복사합니다. 다음 단계에서 필요합니다.


목표를 확인하려면 **진행 상황 확인을 클릭**하세요.
<ql-activity-tracking step=1>
BigQuery External Connection 생성     
</ql-activity-tracking>  

### **1.2 서비스 계정에 IAM 역할 부여**

연결과 연관된 서비스 계정은 **Vertex AI** 및 Cloud Storage에 액세스할 수 있는 권한이 필요합니다.

##### **Vertex AI User / Storage Object Admin 역할 부여**

1. **Navigation menu**(☰)로 이동하여 **IAM 및 관리자 &gt; IAM**을 선택합니다.
2. **+ Grant Access**를 클릭합니다.
3. **New principals** 필드에 이전에 복사한 Service account를 붙여넣습니다.
4. **Select a role** 필드에서 **Vertex AI User** 및 **Storage Object Admin** 역할을 선택합니다.
5. **저장**을 클릭합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task1_iam.png" alt="grant_access.png"  width="348.90" />

목표를 확인하려면 **진행 상황 확인을 클릭**하세요.
<ql-activity-tracking step=2>
생성된 BigQuery External Connection을 위한 서비스 어카운트에 권한 추가
</ql-activity-tracking>

### **1.3 Notebook 업로드**

먼저, 이 작업을 위한 Notebook을 BigQuery Studio에 업로드합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/81603d13197012ee.png" alt="81603d13197012ee.png"  width="348.90" />

1. Google Cloud 콘솔에서 **BigQuery**로 이동합니다.
2. **Explorer** 창에서 **Notebook** 옆의 점 3개(⋮) 아이콘을 클릭하고 **URL에서 Notebook 업로드**를 선택합니다.
3. URL란에 https://github.com/seoeunbae/da-hackerthon-instruction/blob/main/task1_kr.ipynb 를 입력합니다.
4. 나머지 설정에 대해서 default설정을 유지합니다.
5. Notebook을 생성하면 화면 하단 중간에 뜨는, "Go to notebook" 팝업을 클릭합니다.
6. 새 탭에서 Notebook을 엽니다.

### 2. Notebook 설정

### **2.1 환경 초기화**


<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task1_notebool_auth.png" alt="task1_notebool_auth.png" />

노트북 최초 실행 시 위와 같은 팝업 화면이 나타납니다.
위 화면에서 실습계정을 클릭하여, 로그인을 합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task1_notebook1.png" alt="task1_notebook1.png" />

위 이미지의 레드박스에 해당하는, PROJECT_ID를 각각 할당받은 PROJECT_ID로 변경합니다.

> PROJECT_ID는 랩의 화면 왼쪽에서 확인가능합니다. (*하단 이미지 참조)

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task1_project_id.png" alt="task1_project_id.png" width="348.90"/>

다음으로 이제, 설정 셀(Cell)인 첫번째 셀부터 실행합니다. 
이 셀은 필요한 라이브러리를 불러오고 BigQuery 연결을 초기화합니다. 또한 프로젝트 ID, GCS 버킷(Bucket) 경로 등 실습에 필요한 주요 변수를 정의합니다. **반드시 프로젝트 ID를 주어진 환경에 맞게 변경해야 합니다.**

셀 실행은 다음과 같이 왼쪽에 마우스를 올리고 실행 버튼을 클릭하면 실행 시작됩니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task1_cell_execute.png" alt="task1_cell_execute.png"  width="348.90" />

그리고 다음 셀을 실행합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task1_project_setting.png" alt="task1_project_setting.png" />

이 셀을 실행하면 필요한 라이브러리를 불러오고, BigQuery 클라이언트를 초기화하며, 분석을 위한 전역 변수를 설정합니다.


### **2.2 텍스트 리뷰 외부 테이블 생성**

다음으로 BigQuery 외부 테이블(External Table)을 생성합니다. 이 쿼리는 Cloud Storage에 있는 파일을 직접 참조하여 테이블 스키마를 정의합니다.

실행하면 다음과 같은 로그가 출력되며 `customer_reviews_external` 테이블이 생성됩니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task1_external_table.png" alt="task1_external_table.png" />


### **2.3 텍스트 리뷰 테이블 확인**

외부 테이블이 올바르게 생성되었는지 확인합니다. 다음 셀을 실행하여 처음 5개 행을 조회합니다.

```sql
%%bigquery

SELECT * FROM `cymbal.customer_reviews_external`
LIMIT 5
```

> 위 쿼리에서 사용된 `%%bigquery`는 BigQuery Studio에서 BigQuery 쿼리를 실행하는 데 사용되는 매직 커맨드(Magic Command)입니다. 이 매직 키워드를 통해서 실제 Bigquery상에서 Job이 실행됩니다.

출력은 다음과 같습니다: 

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/16fded7203a3a48c.png" alt="16fded7203a3a48c.png"  width="624.00" />

### 3. 데이터 생성

### **3.1 이미지 및 비디오용 객체 테이블 생성**

마찬가지로 비정형 미디어 파일(이미지 및 비디오)을 위한 객체 테이블(Object Table)을 생성합니다. 실행하면 아래와 같은 로그가 출력되며, BigQuery와 Gemini가 해당 파일에 액세스하여 분석할 수 있게 됩니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task1_create_img:video_external.png" alt="task1_create_img:video_external.png" />


### **3.2 BigQuery 객체 테이블 확인**

BigQuery 객체 테이블 확인 셀(이미지 리뷰)을 실행하면 다음과 같은 결과가 출력됩니다: 

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/8df9945a5bc641d9.png" alt="8df9945a5bc641d9.png"  width="624.00" />

BigQuery 객체 테이블 확인 셀(비디오 리뷰)을 실행하면 다음과 같은 결과가 출력됩니다: 

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/6bbca3a1ca6975f4.png" alt="6bbca3a1ca6975f4.png"  width="624.00" />

목표를 확인하려면 **진행 상황 확인을 클릭**하세요.
<ql-activity-tracking step=3>
Create External Review Tables and Upload Data 
</ql-activity-tracking>

### 4. Gemini 모델 생성 및 분석

### **4.1 BigQuery에 Gemini 모델 생성**

이제 BigQuery 데이터셋에 `gemini-2.0-flash` 모델을 등록합니다. 이 단계를 완료하면 SQL 쿼리에서 직접 생성형 모델을 호출할 수 있습니다.


이 SQL 명령어를 실행하면 BigQuery에 원격 모델이 생성됩니다. 생성된 모델은 앞서 설정한 연결을 통해 Gemini Flash 엔드포인트와 통신합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task1_create_model.png" alt="task1_create_model.png" />

목표를 확인하려면 **진행 상황 확인을 클릭**하세요.
<ql-activity-tracking step=4>
Create Gemini Model
</ql-activity-tracking>

### **4.2 텍스트 키워드 및 감성 분석**

데이터와 모델이 준비되었으므로 첫 번째 분석을 시작합니다. 아래 셀을 실행하면 Gemini 모델을 호출하여 각 텍스트 리뷰를 분석하고, 주요 키워드 추출과 감성 분석을 수행합니다. 
이 단계에서는 GCS 버킷에 저장된 텍스트 리뷰 파일(CSV 파일) 을 읽어와서 분석합니다.

> 분석을 완료하는데는 2~3분의 시간이 소요됩니다.

텍스트 감정 분석을 완료하면 다음과 같은 로그가 출력됩니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task1_analyze_text.png" alt="task1_analyze_text.png" />

### **4.3 텍스트 분석 결과 확인**

결과를 검토합니다. 다음 셀은 Gemini가 생성한 키워드와 감성이 포함된 두 테이블의 처음 몇 행을 표시합니다.

```sql
%%bigquery

SELECT * FROM `cymbal.customer_reviews_keywords`
LIMIT 5
```

출력은 다음과 같습니다: 

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/252a4402beffef52.png" alt="252a4402beffef52.png"  width="624.00" />

```sql
%%bigquery
SELECT * FROM `cymbal.customer_reviews_analysis`
LIMIT 5
```

출력은 다음과 같습니다: 

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/9c00564946cfb97d.png" alt="9c00564946cfb97d.png"  width="624.00" />

테이블의 결과를 전체로 확인하려면 다음 버튼을 클릭합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task1_table_full_view.png" alt="task1_table_full_view.png"  width="624.00" />

### **4.5 이미지 및 비디오 분석**

이제 멀티모달 분석을 진행합니다. Gemini에게 리뷰 이미지와 비디오를 분석하여 요약과 키워드를 생성하도록 요청합니다.

실행하면 다음과 같은 로그가 출력되며 분석이 완료됩니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task1_analyze_img:video.png" alt="task1_analyze_img:video.png" />

### **4.6 이미지 및 비디오 분석 샘플 검토**

이 셀은 결과를 더 구체적으로 확인하기 위해 실제 미디어 파일을 AI 생성 분석 결과 아래에 표시합니다. 이를 통해 모델 출력 품질을 확인할 수 있습니다.

이미지 분석 출력 예시:

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/fa1faaa05252648c.png" alt="fa1faaa05252648c.png"  width="624.00" />

비디오 분석 출력 예시:

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/b37008336fda302a.png" alt="b37008336fda302a.png"  width="624.00" />

### **4.7 통합 분석 테이블 생성**

이제 분석 결과를 하나로 통합합니다. 다음 쿼리는 원본 리뷰 데이터와 분석된 테이블(텍스트, 이미지, 비디오)을 조인하여, 하나의 포괄적인 멀티모달 결과 테이블을 생성합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task1_unified_table.png" alt="task1_unified_table.png" />

### **4.8 통합 테이블 확인**

최종 통합 테이블을 확인합니다. 다음 쿼리는 처음 30개 고객 리뷰의 결과를 보여줍니다.

```sql
%%bigquery
SELECT * FROM `cymbal.multimodal_customer_reviews` where video_uri is not null
```

출력은 다음과 같습니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/375c9ea3f9add6e.png" alt="375c9ea3f9add6e.png"  width="624.00" />

목표를 확인하려면 **진행 상황 확인을 클릭**하세요.
<ql-activity-tracking step=5>
Create Multimodal Table
</ql-activity-tracking>

### **5. GenAI로 sentiment 분석 시각화**

이 단계에서는 Notebook에 내장된 생성형 AI 어시스턴트(AI Assist)를 사용하여 차트를 생성합니다.

### [CHALLENGE]
* 출력 결과는 어시스턴트마다 다르게 출력됩니다.
* 이전 단계에서 json포맷이 제대로 파싱되지 않은 경우, 그래프가 나오지 않습니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task1_updated_ui_notebook.png" alt="task1_updated_ui_notebook.png"  width="624.00" />

1. **+ code** 버튼을 클릭하여 새 코드 셀을 추가합니다.
2. 새 셀 안에서 **generate** 버튼을 클릭합니다.
3. 프롬프트 상자에 다음을 주석으로 입력합니다:

```
# multimodal_customer_reviews 테이블의 text_sentiment 분포에 대한 막대 차트를 그려줘

plot a bar chart for the distribution of text_sentiment in the multimodal_customer_reviews table
```

제안된 코드를 수락한 다음 셀을 실행하여 차트를 표시합니다. 이는 전반적인 감성 분석에 대한 개요를 제공합니다. 출력 결과는 다음과 같습니다.
* 출력결과는 실행할 때마다 다르게 생성됩니다.
* 출력 시 오류가 뜨는 경우, 수락 및 실행 버튼을 클릭하여 재실행합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task1_when_error.png" alt="task1_when_error.png"  width="362.50" />


<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/a22946ea304fcf84.png" alt="a22946ea304fcf84.png"  width="362.50" />

### **5.1 감성 분포 시각화 확인**

다음 코드를 실행하여 감성(Sentiment) 데이터가 올바르게 시각화되었는지 확인합니다.

```sql
%%bigquery
SELECT count(customer_id) as count, sentiment_json_string as sentiment FROM `cymbal.multimodal_customer_reviews`
GROUP BY sentiment_json_string
```

#### **Hands-on : GenAI로 플롯 생성하기**

이제 간단한 프롬프트를 작성하여 직접 시각화를 만들어 보겠습니다. 생성형 AI 어시스턴트에게 창의적인 질문을 던져 `table_id_multimodal_reviews` 테이블에서 숨겨진 패턴과 인사이트를 발견해 봅니다.

아래는 몇 가지 예시입니다. 예시를 실행해 보고 직접 질문을 만들어 봅니다. 
**참고:** 생성된 코드에서 오류가 발생하면 코드 셀을 삭제하고 다시 시도합니다.

###### **긍정, 부정, 중립 리뷰의 일일 수를 시간 경과에 따라 추적하는 라인 그래프 생성하기.**
```
# 고객 감성이 날마다 어떻게 변했는지 분석하고 싶어.
# table_id_multimodal_reviews 테이블에서 데이터를 선택하고 긍정, 부정, 중립 감성의 일일 수를 추적하는 라인 차트를 생성해줘.
# 감성은 'sentiment_json_string' 필드에 있고, 날짜는 'review_datetime' 필드에 있어.

I want to analyze how customer sentiment has changed day by day.

Select data from the table_id_multimodal_reviews table and generate a line chart that tracks the daily counts of positive, negative, and neutral sentiments.

The sentiment is in the 'sentiment_json_string' field, and the date is in the 'review_datetime' field.
```

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/e778150e936ed669.png" alt="e778150e936ed669.png"  width="559.28" />

**이미지를 포함한 리뷰의 총 수와 비디오를 포함한 리뷰의 총 수를 비교하는 막대 차트 생성하기.**

```
# table_id_multimodal_reviews를 사용하여 이미지가 있는 리뷰 수와 비디오가 있는 리뷰 수를 세어줘. 결과를 막대 차트로 보여줘.

Using table_id_multimodal_reviews, count the number of reviews that have an image and the number of reviews that have a video. Show the result as a bar chart
```

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/5b8c919c27c77ab.png" alt="5b8c919c27c77ab.png"  width="624.00" />

###### **고객 연령 그룹 '18-29', '30-45', '46-60', '61+'에 대한 긍정, 부정, 중립 리뷰 수를 보여주는 그룹화된 막대 차트 그리기.**

```
# 고객 연령 그룹별로 감성 분석이 필요해.
# 먼저, `table_id_multimodal_reviews` 테이블과 `customers` 테이블을 `customer_id`를 사용해 조인해줘.
# 그런 다음 `age` 열에서 '18-29', '30-45', '46-60', '61+' 네 가지 연령 그룹을 만들어줘.
# 마지막으로, 각 연령 그룹이 '긍정', '부정', '중립' 감성의 총 수를 보여주는 그룹화된 막대 차트를 만들어줘.

I need a breakdown of sentiment by customer age group.
First, join the `table_id_multimodal_reviews` table with the `customers` table using `customer_id`.
Then, create four age groups from the `age` column: '18-29', '30-45', '46-60', and '61+'.
Finally, create a grouped bar chart where each age group shows the total count of 'positive', 'negative', and 'neutral' sentiments.
```

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/6e07c46c14efd6f9.png" alt="6e07c46c14efd6f9.png"  width="624.00" />

###### 모든 성별 카테고리에 걸쳐 긍정, 부정, 중립 리뷰의 총 수를 비교하는 그룹화된 막대 차트 생성하기.

```
# 고객 감성이 성별에 따라 다른지 분석하고 싶어.
# `table_id_multimodal_reviews` 테이블과 `customers` 테이블을 `customer_id`를 사용해 조인해줘.
# 각 성별에 대해 '긍정', '부정', '중립' 리뷰의 총 수를 세어줘.
# 이 비교를 각 성별이 감성에 대한 자체 막대 세트를 갖는 그룹화된 막대 차트로 제시해줘.

I want to analyze if customer sentiment differs by gender.
Join the `table_id_multimodal_reviews` table with the `customers` table using `customer_id`.
For each gender, count the total number of 'positive', 'negative', and 'neutral' reviews.
Present this comparison as a grouped bar chart, where each gender has its own set of bars for the sentiments.
```

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/ddaea34adf51f4e5.png" alt="ddaea34adf51f4e5.png"  width="624.00" />

목표를 확인하려면 **진행 상황 확인을 클릭**하세요.
<ql-activity-tracking step=6>
Visualize Sentiment Trends
</ql-activity-tracking>

> 'Click Check my progress to verify the objective' 아래 각 단계에 있는 활동 추적 버튼을 클릭하여 확인합니다.

모든 단계를 완료했음에도 활동 추적이 성공하지 않으면 잠시 후 다시 시도합니다.

이것으로 Task 1이 완료되었습니다. 멀티모달 고객 리뷰를 성공적으로 분석하고, 인사이트를 통합하여 감성 트렌드를 시각화했습니다.


## Task 2: 고객 세분화를 통한 타겟 마케팅

#### Overview

Task 1에서 리뷰 데이터 분석을 마쳤으므로, 다음 단계인 고객 세분화를 진행합니다. 이번 태스크에서는 Task 1의 인사이트와 고객 인구통계 데이터를 결합합니다. 이어 Gemini를 활용해 각 고객 세그먼트의 상세 페르소나를 생성하고, 이를 타겟 마케팅에 활용합니다.

<div><ql-warningbox>
이번 태스크에서는 BigQuery Studio의 **Notebook** 또는 **Data Canvas** 중 하나를 선택하여 진행합니다.
</ql-warningbox></div>

#### Objective

* 멀티모달 리뷰 분석과 고객 인구통계 데이터 결합
* 연령, 성별, 충성도를 기준으로 고객 세그먼트 정의
* Gemini를 사용하여 각 세그먼트에 대한 풍부하고 상세한 페르소나 생성
* 상세한 세그먼트 인사이트와 페르소나 정의를 담은 결과 테이블 생성

### 1. 태스크 환경 준비

### 1.1 Notebook 업로드

1. BigQuery Studio 탐색기(Explorer) 창에서 **Notebooks** 옆의 점 3개(⋮) 아이콘을 클릭하고 **URL에서 Notebook 업로드(Upload to Notebooks)**를 선택합니다.
<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task2_image7.png" alt="task2_image7.png"  width="624.00" /> 

2. **Upload from**에서 **URL**을 선택하고 https://github.com/cheeunlim/dnpursuit_da_hackathon/blob/main/task2.ipynb 를 입력합니다.
3. Region에서 **us-central-1**을 선택합니다.
<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task2_image1.png" alt="task2_image1.png"  width="624.00" /> 

4. **Upload** 버튼을 클릭한 후, 화면 하단의 **Go to notebook** 알림을 클릭하여 새 탭에서 Notebook을 엽니다. 이 Notebook의 셀(Cell)을 실행하여 Task 2를 진행합니다.

목표를 확인하려면 **진행 상황 확인을 클릭**하세요.
<ql-activity-tracking step=7>
Upload a Notebook on BigQuery Studio
</ql-activity-tracking>

### 1.2 환경 초기화

태스크 설정을 위해 초기 설정 셀을 실행합니다. 이 셀은 필요한 라이브러리를 불러오고 BigQuery 클라이언트를 초기화합니다. 또한 실습에 필요한 주요 변수(예: PROJECT_ID, DATASET_ID)를 정의합니다.
<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task2_image2.png" alt="task2_image2.png"  width="624.00" />

이때 반드시 Project ID를 Qwiklabs 환경에서 제공된 Project ID로 변경해야 합니다.  


### 2. 고객 데이터 EDA 및 고객 세그먼트 세분화 로직 정의

이 단계에서는 Task 1의 `cymbal.multimodal_customer_reviews` 테이블과 `cymbal.customers` 테이블을 `customer_id` 기준으로 조인합니다. 이를 바탕으로 고객 세분화에 필요한 핵심 속성을 탐색합니다. 탐색 과정에서 발견한 유의미한 패턴을 기반으로 세분화 기준과 로직을 정의합니다.

Task 1의 멀티모달 리뷰 분석 결과와 고객 인구통계 데이터를 결합하고, EDA를 수행하여 고객 세분화 핵심 속성을 식별합니다. 이를 바탕으로 다음과 같은 세그먼트 기준을 정의합니다:

* age_group: 40세 미만은 'Younger_Adult', 40세 이상은 'Older_Adult' 
* gender_segment: gender 컬럼의 값을 대문자로 변환 (예: 'MALE', 'FEMALE')
* loyalty_status: loyalty_member가 True면 'LOYAL', False이면 'NON_LOYAL' 
* text_sentiment: sentiment_json_string 컬럼에서 '$.sentiment'를 추출 

* 결과 테이블명 및 컬럼명 규칙:  
최종 생성되는 고객 세그먼트 프로파일 테이블은 `cymbal.unique_segment_profiles`여야 합니다.
이 테이블에는 customer_id, age, gender, loyalty_member, text_sentiment, age_group, gender_segment, loyalty_status, 그리고 이들을 결합한 `persona_age_group_profile` 컬럼이 포함되어야 합니다.  
`persona_age_group_profile` 컬럼은 age_group, gender_segment, loyalty_status 값을 밑줄로 연결하여 "Older_Adult_FEMALE_LOYAL"과 같은 형태로 생성되어야 합니다.  

| **Note:** 이번 단계(2.1 고객 데이터 EDA 및 세분화 로직 정의)는 **Option 1. Notebook**과 **Option 2. Data Canvas** 중 하나를 선택하여 수행합니다. 두 옵션 중 하나만 완료하면 됩니다. **Option 2. Data Canvas**로 진행하려면 **Option 1. Notebook** 내용을 건너뛰고 아래로 이동합니다. |
| :---- |

### 옵션 1: Notebook

Notebook을 선택한 경우, 이전 단계에서 사용한 Notebook에서 작업을 이어갑니다.  
인구통계 정보가 포함된 customers 테이블을 사용합니다. 먼저 테이블 구조를 간단히 살펴보겠습니다.

### 2.1 고객 세그먼트 프로파일 식별

### [CHALLENGE]
이 단계는 여러분이 직접 코드를 작성하여 해결해야 하는 Challenge 단계입니다. 가이드에서 제공하는 힌트를 참고하여 문제를 해결해 보세요.

Notebook의 지침에 따라 `cymbal.multimodal_customer_reviews` 테이블과 `cymbal.customers` 테이블을 `customer_id`를 기준으로 조인하고, 위에서 정의한 age_group, gender_segment, loyalty_status, text_sentiment 컬럼을 생성합니다.  

최종적으로 `persona_age_group_profile` 컬럼을 포함하는 `cymbal.unique_segment_profiles` 테이블을 생성합니다.

**힌트:** Python 클라이언트로 BigQuery 쿼리를 실행하거나, Notebook 내에서 `%%bigquery` 매직 명령어를 활용할 수 있습니다.

**Gemini 활용 가이드:** Gemini에게 테이블 조인, 조건부 로직을 포함한 새 컬럼 생성, 그리고 최종 `persona_age_group_profile` 컬럼을 생성하는 SQL 쿼리를 요청할 수 있습니다.

**결과 확인:** 생성된 `cymbal.unique_segment_profiles` 테이블의 스키마와 데이터를 샘플링하여 컬럼이 예상대로 생성되었는지, `persona_age_group_profile` 형식이 올바른지 확인합니다.

| **Note:** Notebook에서 새 코드 셀을 추가한 다음, Gemini를 통해 시각화 Python 코드를 작성해 봅니다. Gemini에게 위 예시와 유사한 시각화 코드를 요청할 수 있습니다. **Option 1**으로 진행 시, **Task Checkpoint**는 **3. Gemini로 상세 페르소나 생성** 단계 직전에 위치합니다.|
| :---- |

셀 사이 공간에 마우스를 올리고, 나타나는 **+ Code** 버튼을 클릭합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task2_image3.png" alt="task2_image3.png"  width="624.00" />

**generate**를 클릭한 후, 입력창에 Gemini에게 요청할 프롬프트를 입력합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task2_image4.png" alt="task2_image4.png"  width="624.00" />

예시 프롬프트:
'BigQuery에서 `cymbal.multimodal_customer_reviews` 테이블과 `cymbal.customers` 테이블을 `customer_id`로 조인하는 코드를 작성해 줘.'

프롬프트를 입력하고 Enter 키를 눌러 전송합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task2_image5.png" alt="task2_image5.png"  width="624.00" />

요청한 프롬프트에 대해 Gemini가 생성한 코드를 실행할 수 있습니다.

### 2.2 시각화

이 단계에서는 자유롭게 데이터를 탐색하며, 앞서 생성한 `unique_segment_profiles` 테이블과 `customers` 테이블을 살펴봅니다. 

이 섹션의 코드 셀은 각 세그먼트에 속하는 고객 수를 시각화합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/4a0efc497af584e5.png" alt="4a0efc497af584e5.png"  width="624.00" />

위와 같은 막대 차트로 고객 세그먼트 집계를 확인할 수 있습니다.

### 2.3 추가 시각화

다음 주제로 EDA를 자유롭게 수행해 봅니다:

**주제 1**: 연령대와 충성도 간의 관계 분석
`age_group`과 `loyalty_status` 조합이 고객 수에 미치는 영향을 시각화하여, 특정 연령대 고객의 충성도 경향을 분석합니다.

**주제 2**: 세그먼트별 지리적 분포 시각화
`address_city` 정보를 활용하여 각 `persona_age_group_profile`별 고객의 지리적 분포(특정 도시 집중 여부 등)를 파악합니다.

| **2. 고객 데이터 EDA 및 고객 세그먼트 세분화 로직 정의**를 완료했습니다. **Option 2. Data Canvas** 내용은 건너뛰고, 아래로 스크롤하여 **3. Gemini로 상세 페르소나 생성** 단계를 진행합니다. |
| :---- |
  
  
### 옵션 2: Data Canvas

BigQuery Data Canvas는 시각적 인터페이스를 통해 복잡한 데이터 조인, 변환, 집계, 시각화를 수행할 수 있는 도구입니다. 이 옵션을 선택하면 고객 데이터 EDA 및 세그먼트 생성 작업을 시각적으로 진행할 수 있습니다.

### 2.1 Data Canvas에 테이블 추가 및 조인 수행

화면 왼쪽 탐색 패널에서 **Data Canvas**를 클릭하거나, **+** 버튼을 눌러 새 캔버스(Canvas)를 생성합니다.

Data Canvas 화면의 **Recents** 아래에서 `multimodal_customer_reviews` 테이블과 `customers` 테이블을 각각 캔버스에 추가합니다. 테이블 추가 방법은 다음과 같습니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/f50d21a3cf64f30.png" alt="f50d21a3cf64f30.png"  width="621.50" />
<!-- #TODO: Update table screenshot -->

**Recents** > **customers** 테이블을 클릭합니다.  
**Recents**에 `customers` 테이블이 보이지 않는 경우, 좌측 패널의 **Classic Explorer**에서 **cymbal** 데이터셋을 펼칩니다.  
**customers** 테이블 옆의 **더보기(세로 점 3개) > Open in > Current data canvas**를 클릭합니다.  
<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task2_image8" alt="task2_image8"  width="511.50" />

같은 방법으로 `multimodal_customer_reviews` 테이블을 추가합니다.  

`customers` 테이블 구조를 간단히 살펴봅니다. **Preview**를 클릭하면 테이블 데이터를 일부 조회할 수 있습니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/99c8631e4ad02846.png" alt="99c8631e4ad02846.png"  width="624.00" />

### 2.2 고객 세그먼트 프로파일 식별

`customers` 테이블 노드를 클릭하고 **Join** 옵션을 선택합니다. 이어 **On this canvas** 아래의 `multimodal_customer_reviews` 테이블 노드를 선택하고 **OK**를 클릭합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/92fe9ee2bd93718.png" alt="92fe9ee2bd93718.png"  width="500.50" />

두 테이블 사이에 Join 블록이 생성되면, 조인 조건(예: `ON t1.customer_id = t2.customer_id`)이 올바른지 확인합니다. 

확인 후 **Run** 버튼을 클릭합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/eb3142b78394b062.png" alt="eb3142b78394b062.png"  width="519.69" />

조인된 결과 블록을 클릭하고 **Query these results**를 선택합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/762ca3c5e22064c3.png" alt="762ca3c5e22064c3.png"  width="527.50" />

새 쿼리 편집기 창에서 직접 SQL을 작성하거나, Gemini에게 자연어 프롬프트로 컬럼 생성을 요청합니다.

* sentiment_json_string 컬럼에서 '$.sentiment'를 추출하여 'text_sentiment'라는 새 컬럼을 만듭니다.
* age 컬럼을 기반으로 40세 미만은 'Younger_Adult', 40세 이상은 'Older_Adult'로 구분하는 'age_group' 컬럼을 추가합니다.
* gender 컬럼의 값을 대문자로 변환하여 'gender_segment'라는 컬럼으로 만듭니다.
* loyalty_member가 True면 'LOYAL', False이면 'NON_LOYAL'인 'loyalty_status' 컬럼을 추가합니다.

**Gemini 활용 가이드:**   
Gemini에게 자연어 프롬프트로 위 요구사항을 충족하는 SQL 쿼리 생성을 요청할 수 있습니다.  
예를 들어: 'gender 컬럼 값은 대문자로 변환해 `gender_segment` 컬럼으로 만들고, `loyalty_member`가 True면 `LOYAL`, False면 `NON_LOYAL`인 `loyalty_status` 컬럼을 추가해 줘.'  
쿼리를 실행하여 결과를 확인하고, 새 컬럼들이 예상대로 생성되었는지 검토합니다.  

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/bbb2804f0f932c0e.png" alt="bbb2804f0f932c0e.png"  width="624.00" />

**Run** 버튼을 클릭하여 쿼리를 실행하고 결과를 확인합니다.

이제 위에서 생성한 `age_group`, `gender_segment`, `loyalty_status` 컬럼을 연결하여 `Older_Adult_FEMALE_LOYAL`과 같은 형태의 고객 세그먼트 컬럼을 생성합니다.

마지막 작업 노드를 다시 클릭하고 하단의 **Query these results** 버튼을 클릭합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/371f475802604197.png" alt="371f475802604197.png"  width="544.50" />

앞서 생성한 `age_group`, `gender_segment`, `loyalty_status` 컬럼을 연결하여 `Older_Adult_FEMALE_LOYAL` 형태의 새 컬럼을 생성합니다. 
이때 최종 컬럼명은 반드시 `persona_age_group_profile`이어야 합니다.  

Gemini에게 자연어 프롬프트로 그룹화 및 프로파일 생성 쿼리를 요청하거나 직접 SQL을 작성합니다.  

쿼리를 실행하여 고유 페르소나 프로파일 컬럼이 의도한 형식대로 생성되었는지 확인합니다.  

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/c3c46a68c5b74c68.png" alt="c3c46a68c5b74c68.png"  width="624.00" />

### 2.3 결과 테이블 저장

최종 쿼리 결과(Query Results) 블록을 클릭합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/e26160370276cd5d.png" alt="e26160370276cd5d.png"  width="624.00" />

상단 또는 하단에 있는 **Save results &gt; BigQuery table**을 선택합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/3197356bdb34b06a.png" alt="3197356bdb34b06a.png"  width="624.00" />

**Dataset**은 `cymbal`, **Table name**은 `unique_segment_profiles`로 지정합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/1b96c93297ec2f27.png" alt="1b96c93297ec2f27.png"  width="508.50" />

**Save** 버튼을 클릭하여 테이블을 저장합니다.

이 작업으로 Data Canvas에서 정의한 고유 세그먼트 프로파일이 포함된 `cymbal.unique_segment_profiles` 테이블이 BigQuery에 생성됩니다.  

### 2.4 시각화 블록 추가 (선택 사항)

최종 쿼리 결과 블록을 클릭하고 **Visualize**를 선택합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/7db194431f406e7b.png" alt="7db194431f406e7b.png"  width="624.00" />

다양한 차트 유형(예: Bar Chart, Pie Chart)을 사용하여 고객 분포를 시각적으로 탐색합니다. 이를 통해 어떤 세그먼트에 고객이 가장 많은지 등을 파악할 수 있습니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/86ffa77b507ffa68.png" alt="86ffa77b507ffa68.png"  width="491.10" />

위 예시는 Auto-generate 기능을 사용해 자동으로 생성된 차트로 LOYAL, NON_LOYAL 고객의 세그먼트별 분포를 나타냅니다.  

목표를 확인하려면 **진행 상황 확인을 클릭**하세요.
<ql-activity-tracking step=8>
Create tables for Customer Personas
</ql-activity-tracking>

### 3. Gemini로 상세 페르소나 생성

이전 단계에서 `cymbal.unique_segment_profiles` 테이블을 생성했다면, 이제 이 테이블을 기반으로 Gemini 모델을 호출하여 각 세그먼트의 상세 페르소나를 생성합니다. 페르소나를 생성함으로써 단순한 인구통계학적 분류를 넘어, 고객의 행동 패턴과 성향을 깊이 있게 이해하고 이를 바탕으로 더욱 정교한 개인화 마케팅 전략을 수립할 수 있습니다.

Notebook의 'Gemini를 사용한 상세 페르소나 생성' 헤더 아래 셀부터 실행합니다. 앞서 정의한 각 페르소나에 대해 BigQuery ML의 `ML.GENERATE_TEXT` 함수와 Gemini 모델을 활용하여 다각적인 분석을 수행하고, 결과를 `cymbal.segment_level_gemini_analysis` 테이블에 저장합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task2_image6.png" alt="task2_image6.png"  width="624.00" />

| **참고**: 이후 단계는 BigQuery ML의 ML.GENERATE_TEXT 함수를 반복적으로 호출하는 로직이 필요하며, 현재 BigQuery Data Canvas 인터페이스 내에서 직접적으로 이 반복 호출을 구성하기 어렵습니다. 따라서 남은 태스크는 BigQuery Studio의 Python 노트북 셀에서 실행하는 것을 권장합니다.|
| :---- |

앞에서 정의한 각각의 페르소나에 대해 BigQuery ML의 ML.GENERATE_TEXT 함수와 Gemini 모델을 사용해 다각적인 페르소나 분석을 생성하고, 출력값을 cymbal.segment_level_gemini_analysis 테이블에 저장합니다.



### 4. 최종 고객 인사이트 및 페르소나 정의 테이블 생성

마지막으로 주요 인사이트와 정리된 페르소나 설명을 담은 테이블을 생성합니다. 해당 코드는 제공되어 있으므로 Notebook에서 실행합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/bdf6585a0a125882.png" alt="bdf6585a0a125882.png"  width="624.00" />

목표를 확인하려면 **진행 상황 확인을 클릭**하세요.
<ql-activity-tracking step=9>
Create tables for Persona Segment Descriptions
</ql-activity-tracking>



## Task 3: 불만족 고객을 위한 맞춤형 프로모션 상품 추천 및 평가


#### Overview

Task 3에서는 Task 1의 불만족 리뷰와 Task 2의 고객 세그먼트 및 페르소나 정보를 활용합니다. 불만족 고객의 재참여를 유도하기 위해 개인화된 상품을 추천하고, 이를 평가합니다.

#### Objective

* 감성 분석 기반 부정적 피드백 고객 식별
* 식별된 고객의 세그먼트 프로파일 및 지리 정보 검색
* 각 세그먼트 및 지역 내 최고 인기 구매 제품을 조회하는 SQL 쿼리 작성
* 맞춤형 프로모션 제품 추천
* Gemini를 사용해 제품 추천을 평가하는 Python 코드 및 SQL 쿼리 작성

<div><ql-warningbox>
참고: Task 3은 Task 1, 2에 의존도를 가지고 있어 Task 1, 2를 반드시 완수한 후에 시작해야 합니다.
</ql-warningbox></div>

#### Objective

* 리뷰 데이터와 추천 데이터 결합
* 타겟 고객 리스트 생성
* 개인화 마케팅 컨텐츠 생성

### 1. 태스크 환경 설정

### 1.1 Notebook 업로드

1. BigQuery Studio 탐색기 창에서 **Notebooks** 옆의 점 3개(⋮) 아이콘을 클릭하고 **URL에서 Notebook 업로드(Upload notebook from URL)**를 선택합니다.
<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task2_image1.png" alt="task2_image1.png"  width="624.00" />

2. **Upload from**에서 **URL**을 선택하고 https://github.com/cheeunlim/dnpursuit_da_hackathon/blob/main/task3.ipynb 를 입력합니다.
3. Region: us-central-1을 선택합니다.
4. **Upload** 버튼을 클릭한 후, 화면 하단의 **Go to notebook** 알림을 클릭하여 새 탭에서 Notebook을 엽니다. 이 Notebook의 셀을 실행하여 Task 6을 진행합니다.

### 1.2 환경 초기화

태스크 설정을 위해 초기 설정 셀을 실행합니다. 이 셀은 필요한 라이브러리를 불러오고 BigQuery 클라이언트를 초기화합니다. 또한 실습에 필요한 주요 변수(예: PROJECT_ID, DATASET_ID)를 정의합니다.

### 2. 타겟 고객 리스트 생성

이 단계에서는 Task 1의 `final_customer_insights` 테이블과 Task 5의 `product_recommendations` 테이블을 조인하여, 각 고객별 추천 상품 정보를 포함하는 `customer_marketing_target` 테이블을 생성합니다.
제공된 코드를 실행하고 다음 단계로 넘어갑니다.

### 3. 불만족 고객의 세그먼트 및 지리적 데이터 검색

Step 2에서 식별한 불만족 고객 데이터를 가져옵니다.  
  
`customers` 테이블에서 `address_city`를, `final_customer_insights`에서 `persona_age_group_profile` 정보를 추출하여 `negative_customer_segment_data` 테이블에 저장합니다.
  
  
목표를 확인하려면 **진행 상황 확인을 클릭**하세요.
<ql-activity-tracking step=10>
  Create tables for Negative Customer Segments
</ql-activity-tracking>

### 4. 세그먼트, 도시별 인기 제품 추출 및 최종 추천 상품 결정

### [CHALLENGE] 
이 단계는 여러분이 직접 코드를 작성하여 해결해야 하는 Challenge 단계입니다. 가이드에서 제공하는 힌트를 참고하여 문제를 해결해 보세요.

불만족 고객이 속한 세그먼트(`persona_age_group_profile`)와 거주 도시(`address_city`)에서 다른 고객들이 가장 많이 구매한 제품을 조회하여 개인화 추천에 활용합니다. 각 세그먼트별 상위 2개 인기 제품과 도시별 상위 2개 인기 제품을 각각 `cymbal.segment_top_products_ranked` 테이블과 `cymbal.city_top_products_ranked` 테이블에 저장하는 SQL 쿼리를 작성합니다.

**요구사항**:

* `cymbal.segment_top_products_ranked` 테이블:  
컬럼명: `persona_age_group_profile`, `segment_top1_product`, `segment_top2_product`. 
`final_customer_insights` 테이블과 `customer_reviews_external` 테이블을 조인하여 사용합니다.  
`ROW_NUMBER()` 윈도우 함수를 사용하여 `persona_age_group_profile`별 `product_id` 구매 횟수(`COUNT(product_id)`)에 따라 순위를 매깁니다.   
상위 1위(rn=1)와 2위(rn=2) 제품의 `product_id`를 추출하여 각각 `segment_top1_product`, `segment_top2_product` 컬럼에 저장합니다.   
* `cymbal.city_top_products_ranked` 테이블:  
컬럼명: `address_city`, `city_top1_product`, `city_top2_product`.  
`customers` 테이블과 `customer_reviews_external` 테이블을 조인하여 사용합니다.   
`ROW_NUMBER()` 윈도우 함수를 사용하여 `address_city`별 `product_id` 구매 횟수(`COUNT(product_id)`)에 따라 순위를 매깁니다.   
상위 1위(rn=1)와 2위(rn=2) 제품의 `product_id`를 추출하여 각각 `city_top1_product`, `city_top2_product` 컬럼에 저장합니다.   

**힌트:** 두 쿼리 모두 `WITH` 절을 사용하여 순위가 매겨진 중간 결과를 생성하고, 이후 `MAX(CASE WHEN ... END)` 패턴으로 피벗하여 최종 컬럼을 만들 수 있습니다.


목표를 확인하려면 **진행 상황 확인을 클릭**하세요.
<ql-activity-tracking step=11>
Create tables for Top Products
</ql-activity-tracking>

<!-- #TODO: Add screenshot of final recommendation table example -->

각 상품 테이블을 병합하고, 고객이 이미 리뷰를 남긴 상품과의 중복을 확인합니다. 고객이 리뷰한 상품이 추천 목록에 있다면 제외하고, 없다면 거주 도시 기반의 두 번째 추천 상품을 제외하여 총 3개의 추천 상품 목록으로 정리합니다.

이 단계의 코드는 제공되므로 그대로 실행하여 `cymbal.final_personalized_recommendations` 테이블을 생성합니다.


목표를 확인하려면 **진행 상황 확인을 클릭**하세요.
<ql-activity-tracking step=12>
Create tables for Personalized Recommendations
</ql-activity-tracking>

### 5. Gemini로 개인화된 추천 콘텐츠 평가

### [CHALLENGE]
이 단계는 여러분이 직접 코드를 작성하여 해결해야 하는 Challenge 단계입니다. 가이드에서 제공하는 힌트를 참고하여 문제를 해결해 보세요.

이제 고객별 추천 상품의 적합도를 평가합니다. 평가는 BigQuery에서 Gemini를 활용하며, 고객 세그먼트 페르소나, 추천 상품 이름, 카테고리 정보를 기반으로 진행합니다.

**요구사항**:   
1. **`cymbal.temp_recommendation_details` 테이블 생성 (제공된 코드 실행)**: 이 테이블을 먼저 생성하여 필요한 데이터를 준비합니다.  
2. **평가 프롬프트 생성 (Python 코드 작성)**: 각 추천 항목(테이블의 행)마다 Gemini 평가 프롬프트를 동적으로 생성하는 Python 코드를 작성합니다. `GEMINI_EVALUATION_PROMPT_TEMPLATE` 변수를 활용하여 `prompt` 컬럼을 생성합니다.  

    * GEMINI_EVALUATION_PROMPT_TEMPLATE:  

      ```python
      GEMINI_EVALUATION_PROMPT_TEMPLATE = """
      당신은 고객 세그먼트 분석가입니다. 다음 정보를 기반으로, 추천 상품이 고객 세그먼트 페르소나에 얼마나 적합한지 평가하는 JSON 객체를 생성하세요.

      1.  고객 세그먼트 페르소나 (분석 결과): {persona_analysis}
      2.  추천 상품 이름: {product_title}
      3.  추천 상품 카테고리: {product_categories}

      JSON 형식 제약조건:
      * "product_title" (상품 이름)
      * "product_categories" (상품 카테고리)
      * "compatibility_score" (페르소나 대비 적합성 점수, 1에서 100 사이의 정수)
      * "reasoning" (점수를 부여한 근거, 50단어 이내)
      * 출력은 JSON 객체 하나여야 합니다.
      """
      ```

    * 결과: 생성된 프롬프트 DataFrame은 `customer_id`, `recommendation_rank`, `product_title`, `prompt` 컬럼을 포함해야 합니다.

3. **프롬프트 저장**: 생성된 프롬프트 DataFrame을 `temp_gemini_evaluation_prompts`라는 임시 BigQuery 테이블에 저장합니다.
4. **Gemini 모델 호출 및 평가 테이블 생성 (SQL 쿼리 작성)**: `temp_gemini_evaluation_prompts` 테이블의 프롬프트를 사용하여 BigQuery ML의 `ML.GENERATE_TEXT` 함수를 호출하는 SQL 쿼리를 작성합니다.

* JSON 결과 파싱 및 최종 테이블(`cymbal.gemini_recommendation_evaluation`) 생성 로직은 제공되므로 이를 활용하여 쿼리를 완성합니다.
* 모델 이름: `GEMINI_MODEL_NAME` 변수를 사용합니다.
* STRUCT 옵션: `STRUCT(0.5 AS temperature, 1024 AS max_output_tokens, TRUE AS flatten_json_output)`를 사용합니다.

모든 과정을 마치고, Gemini의 평가 내용을 조회합니다.  

**진행 상황 확인을 클릭**하세요.
<ql-activity-tracking step=13>
Create tables for Recommendation Evaluations
</ql-activity-tracking>

### **Evaluation 결과 확인 및 다음 단계**

생성된 `compatibility_score`를 확인해 보면, 점수가 예상보다 높지 않거나(예: 60~70점대) 추천의 정확도가 다소 부족할 수 있습니다. 이는 룰 베이스(Rule-based) 추천이나 단순한 통계적 접근의 한계일 수 있습니다.

따라서 우리는 더 정교하고 개인화된 추천을 위해 **Machine Learning** 기반의 추천 모델을 도입할 필요가 있습니다. 다음 **Task 4**와 **Task 5**에서는 BQML을 활용하여 고도화된 상품 추천 모델을 구축하고, 이를 통해 추천의 품질을 획기적으로 개선해 보겠습니다.

마지막으로 `cymbal.gemini_recommendation_evaluation` 테이블에서 `compatibility_score`를 기준으로 내림차순 정렬하여 결과를 미리 확인해 봅니다.

## Task 4: 추가적인 탐색적 데이터 분석(EDA)

#### Overview

불만족스러운 경험을 한 고객에게 더 나은 맞춤형 추천 서비스를 제공하고자 합니다. 상품 추천 모델 생성 및 훈련을 위해 탐색적 데이터 분석(EDA)을 수행하며, 다음 방법을 활용합니다.

1. BigQuery Studio + ML Model을 활용한 탐색적 데이터 분석  
2. \[선택사항\] Data Insights와 Data Canvas를 활용한 탐색적 데이터 분석

불만족 고객이 만족할 수 있는 상품 추천 모델을 만들기 위해 탐색적 데이터 분석을 진행해야 합니다. 아래 챌린지를 해결하며 자유롭게 분석을 진행합니다.

| **Note:** 이번 단계는 Task 5 상품 추천 모델을 위한 데이터 분석이 목표입니다. 아래 가이드는 예시일 뿐이므로, 방법을 숙지한 후 비즈니스에 어떻게 적용할지 고민하며 자유롭게 분석을 진행합니다. 또한 옵션에 제한되지 않고 자유롭게 조합하여 분석할 수 있습니다. |
| :---- |


#### Objective

* BigQuery Studio와 ML Model을 활용한 데이터 분석을 위해 다양한 모델과 활용 방법을 경험합니다.  
* \[선택사항\] Data Insights와 Data Canvas를 활용한 데이터 분석을 경험합니다.  
* 분석 결과를 CSV 파일로 Google Cloud Storage 버킷(Bucket)에 저장합니다. 


### 1. 고객 리뷰 테이블을 cymbal 데이터세트 내에 생성하기

다음은 고객 리뷰 테이블을 `cymbal` 데이터셋에 생성하는 과정입니다. 테이블명은 `customer_review`로 지정합니다.

1. 메인 상단 중앙의 검색창에서 **BigQuery**를 검색하고 클릭합니다.
    <img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task4_img_000.png" alt="task4_img_000.png" />  


2. 빅쿼리 콘솔 내 Explorer 패널에서 아래와 같이 Tree 모양의 아이콘을 클릭 후 이미지와 같이 나타납니다. 이때, Cymbal 옆 점 3개(⋮) 아이콘을 클릭 후 Create table 버튼을 클릭합니다.

    <img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task4_img_001.png" alt="task4_img_001.png" />  

3. 아래와 같이 값을 설정합니다.
    <img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task4_img_002.png" alt="task4_img_002.png" />  

    > **Source**: **Google Cloud Storage**를 선택하고, **Browse**를 클릭하여 `your-project-id-bucket/review/customer_reviews.csv` 경로를 지정합니다.  
    > (`{your-project-id}-bucket`, 예: `qwiklabs-gcp-xx-xxxxx-bucket`)  
    > **Destination**: **Table** 이름을 `customer_review`로 입력합니다.  
    > **Schema**: **Auto detect**를 선택합니다.  


4. **Advanced options**를 클릭하여 아래와 **Quoted newlines**만 설정한 후 테이블을 생성합니다. 그 외 옵션은 default로 둡니다. 
    <img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task4_img_003.png" alt="task4_img_003.png" />  

    | **Note:** Quoted newlines는 데이터 무결성을 확보하는 데 중요한 옵션입니다. 따옴표\(""\)로 감싸진 데이터 내 줄개행을 행의 끝으로 인식하지 않고 텍스트 자체에 줄바꿈을 할 수 있도록 합니다.  |
    | :---- |

5. 결과적으로 `cymbal` 데이터셋 아래에 `customer_review` 테이블이 생성됩니다.    
    <img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task4_img_004.png" alt="task4_img_004.png" />


### [선택사항] Data Insights와 Data Canvas를 활용한 탐색적 데이터 분석

다음은 Data Insights와 Data Canvas의 생성 및 사용 방법을 소개합니다. 이후 분석 과정에서 자유롭게 활용해 봅니다. 해당 단계를 건너뛰려면 아래 단계로 이동합니다.  

1. Data Insights 다루기  
    > 왼쪽 패널에서 `customer_review` 테이블을 클릭합니다. 아래 이미지와 같이 **Insights** 버튼을 클릭하고 **Generate Insights for free**를 선택합니다. 이때, 시간이 다소 소요될 수 있습니다. 
    <img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task4_img_005.png" alt="task4_img_005.png" />  

    > 다음과 같은 결과가 나타납니다. 결과는 실행 시점마다 다를 수 있습니다.  
    <img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task4_img_006.png" alt="task4_img_006.png" />  

    > 분석하고자 하는 항목의 복사 버튼을 클릭하여 쿼리를 복사합니다. 
    <img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task4_img_007.png" alt="task4_img_007.png" />  

    > 쿼리 창에 복사한 쿼리를 붙여넣고 **Run** 버튼을 클릭하여 결과를 확인합니다.  
    <img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task4_img_008.png" alt="task4_img_008.png" />  


2. Data Canvas 다루기  
    > BigQuery Studio 홈에서 **Data Canvas** 버튼을 클릭합니다.  
    <img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task4_img_009.png" alt="task4_img_009.png" />  

    > \[해당하는 경우\] **Region**을 `us-central1`로 설정하고 **Save**를 클릭합니다.  
    <img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task4_img_010.png" alt="task4_img_010.png" />  

    > Data Canvas 초기 화면이 나타납니다. 이 예제에서는 자연어로 테이블을 쿼리합니다. 먼저 **Search for data**를 클릭합니다.   
    <img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task4_img_011.png" alt="task4_img_011.png" />  

    > `customer_review`를 검색하여 선택한 뒤 **Query** 버튼을 클릭합니다.
    <img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task4_img_012.png" alt="task4_img_012.png" />  

    > '부정적인 리뷰를 남긴 고객들을 보고 싶어'라고 자연어 질의를 입력한 후 **Generate** 버튼을 클릭합니다. 생성된 쿼리를 확인하고 **Run** 버튼을 클릭하여 결과를 확인합니다.  
    <img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task4_img_013.png" alt="task4_img_013.png" />  

    > 빨간 박스 안의 버튼들을 활용하여 추가적인 데이터 분석을 진행할 수 있습니다.  
    <img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task4_img_014.png" alt="task4_img_014.png" />


### BigQuery Studio와 ML Model을 활용한 탐색적 데이터 분석

다음은 ML Model을 생성하여 데이터 분석에 활용하는 예제입니다. 이 예제는 참고용이므로, 사용 방법을 숙지한 후 자유롭게 모델을 선택하여 데이터를 분석합니다. 지원 모델은 다음 [링크](https://cloud.google.com/bigquery/docs/bqml-introduction?hl=ko#generative_ai_and_pretrained_models)를 참조합니다.

#### Overview 

1. 고객, 고객 리뷰, 상품 데이터 분석을 진행합니다.  
    > 옵션인 Data Insights와 Data Canvas도 활용해 봅니다.  
    > 임베딩을 활용하여 품질 문제, 배송 문제 등을 검색하고 분석합니다.  
    
2. 이 예제에서는 다음 데이터를 추가합니다.  
    > 다음과 같이 데이터를 보강합니다.  
    > **품질 문제, 배송 문제 등으로 군집화된 분류 데이터**  
    > **고객들의 리뷰에서 개선점과 고객이 좋아할 특성을 정의한 데이터**  
    > **제품 테이블에 제품의 장점, 단점 그리고 특성을 정의한 데이터**  


### **임베딩과 유사도 검색을 활용한 분석**


1. BigQuery Studio 메인 화면에서 **ML Models**를 클릭합니다.  
    <img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task4_img_015.png" alt="task4_img_015.png" />

2. **Dataset name**은 `cymbal`, **Model ID**는 `embedding_model`로 지정하고 **Continue**를 클릭합니다.  
    <img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task4_img_016.png" alt="task4_img_016.png" />  


3. **Creation method**에서 **Connect to Vertex AI LLM service and CloudAI services**를 선택하고 **Continue**를 클릭합니다.  
    <img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task4_img_017.png" alt="task4_img_017.png" />  


4. **Model options**에서 다음과 같이 설정한 후 **Create model**을 클릭합니다.  
    > **Model type** : Google and Partner Models  
    > **Remote Connection** : Default connection  
    > **ENDPOINT** : text-multilingual-embedding-002

    <img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task4_img_018.png" alt="task4_img_018.png" />  

    | Note: text-multilingual-embedding-002는 다국어 대상으로 임베딩을 생성하는 모델입니다. 텍스트 임베딩 지원 모델에 관한 자세한 내용은 [링크](https://cloud.google.com/vertex-ai/generative-ai/docs/model-reference/text-embeddings-api?hl=ko) 참조하세요. |
    | :---- |


5. 빨간 박스 안의 버튼을 클릭하여 새 쿼리 창을 열고 다음 단계를 진행합니다.  
    <img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task4_img_019.png" alt="task4_img_019.png" />  


6. 다음 쿼리에서 `PROJECT_ID` 값을 수정하여 임베딩 테이블을 생성합니다.
    
    ```sql
    CREATE OR REPLACE TABLE `PROJECT_ID.cymbal.embeded_table` AS
    SELECT * FROM ML.GENERATE_EMBEDDING(
    MODEL `PROJECT_ID.cymbal.embedding_model`,
    (SELECT review_text AS content, customer_id, customer_review_id
    FROM `PROJECT_ID.cymbal.customer_review`)
    )
    ```
    
    > `PROJECT_ID`: 현재 프로젝트 ID  

7. **Run** 버튼을 클릭하여 실행합니다.
8. 임베딩 결과 테이블을 바탕으로 벡터 검색을 수행합니다. 아래 쿼리는 품질 불량으로 추정되는 항목을 유사도 순으로 보여줍니다. 실행 후 결과가 실제와 일치하는지 검증합니다.
    <img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task4_img_020.png" alt="task4_img_020.png" />  

    | Note: 컬럼의 양이 많다면 벡터 색인을 사용할 수 있습니다. 자세한 정보는 [링크](https://cloud.google.com/bigquery/docs/vector-index?hl=ko) 참조하세요.  |
    | :---- |

    ```sql
    SELECT
    t.content,
    t.customer_id,
    t.customer_review_id,
    # 코사인 유사도 점수 (1에 가까울수록 의미적으로 유사함)
    1 - ML.DISTANCE(
    t.ml_generate_embedding_result,
    query_embed.ml_generate_embedding_result,
    'COSINE'
    ) AS similarity_score
    FROM
    `PROJECT_ID.cymbal.embeded_table` AS t,
    ML.GENERATE_EMBEDDING(
    MODEL `PROJECT_ID.cymbal.embedding_model`,
    (SELECT '품질 불량' AS content) # 여기에 검색어를 입력합니다.
    ) AS query_embed
    ```  

    > `PROJECT_ID`: 현재 프로젝트 명


### [CHALLENGE]
품질 불량 유사도를 내림차순으로 정렬하여 상위 5개 항목을 출력합니다.


결과의 예는 다음처럼 나오게 됩니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task4_img_021.png" alt="task4_img_021.png" />

<!-- 목표를 확인하려면 **진행 상황 확인을 클릭**하세요.
<ql-activity-tracking step=14>
Challenge 쿼리 실행 여부 확인
</ql-activity-tracking> -->

### **각 리뷰별 카테고리 군집화 데이터 보강**

1. BigQuery Studio 메인화면에서 ML model를 클릭합니다.  
    <img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task4_img_015.png" alt="task4_img_015.png" />

2. 데이터세트(Dataset name)는 cymbal로 지정 및 모델명은 gemini\_model 로 작성 후 Continue 클릭합니다.  
    <img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task4_img_022.png" alt="task4_img_022.png" />  


3. Creation method에서 **Connect to Vertex AI LLM service and CloudAI services** 체크 후 Continue 클릭합니다.  
    <img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task4_img_017.png" alt="task4_img_017.png" />  


4. Model options에서 다음과 같이 설정 후 생성합니다.  
    > **Model type** : Google and Partner Models  
    > **Remote Connection** : Default connection  
    > **ENDPOINT** : gemini-2.5-flash  

    <img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task4_img_023.png" alt="task4_img_023.png" />  

    | Note: gemini-2.5-flash는 생성형 AI로 다양한 역할을 합니다. 자세한 내용은 [링크](https://cloud.google.com/vertex-ai/generative-ai/docs/models/gemini/2-5-flash?hl=ko) 참조하세요 |
    | :---- |

5. 이제 고객 리뷰를 참고하여 군집화된 카테고리를 생성합니다.  
6. 새 쿼리 창을 열고, 다음 쿼리에서 `PROJECT_ID` 를 설정하여 실행합니다.

    ```sql
    SELECT
    customer_review_id,
    customer_id,
    issue_category
    FROM
    AI.GENERATE_TABLE(
    MODEL `PROJECT_ID.cymbal.gemini_model`,
    (
    SELECT
    t.customer_id,
    t.customer_review_id,
    t.review_text,
    CONCAT(
    '''
    부정적인 리뷰로 판단된 경우 분석해서 문제점을 카테고리화해서 분류하려고 해
    . 다음 카테고리 중 하나를 택해서 분류해줘
    **품질불량, 배송문제, 기대미충족, 단순변심, 결제문제, 응대문제, 기타**
    긍정적인 리뷰로 판단된 경우 **해당없음**으로 분류해주면 돼.
    분석할 리뷰:
    ''',
    t.review_text
    ) AS prompt
    FROM
    `PROJECT_ID.cymbal.customer_review` AS t
    WHERE
    t.rating < 3
    ),
    STRUCT(
    "issue_category STRING" AS output_schema,
    8192 AS max_output_tokens)
    )
    ```

    > `PROJECT_ID`: 현재 프로젝트 명  

    실행하면 다음과 같은 결과가 출력됩니다.

    <img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task4_img_024.png" alt="task4_img_024.png" />

7. 결과를 테이블에 추가합니다.

    ```sql
    CREATE OR REPLACE TABLE `PROJECT_ID.cymbal.cgenf_table` AS (
    SELECT
    customer_review_id,
    customer_id,
    issue_category
    FROM
    AI.GENERATE_TABLE(
    MODEL `PROJECT_ID.cymbal.gemini_model`,
    (
    SELECT
    t.customer_id,
    t.customer_review_id,
    t.review_text,
    CONCAT(
    '''
    부정적인 리뷰로 판단된 경우 분석해서 문제점을 카테고리화해서 분류하려고 해
    . 다음 카테고리 중 하나를 택해서 분류해줘
    **품질불량, 배송문제, 기대미충족, 단순변심, 결제문제, 응대문제, 기타**
    긍정적인 리뷰로 판단된 경우 **해당없음**으로 분류해주면 돼.
    분석할 리뷰:
    ''',
    t.review_text
    ) AS prompt
    FROM
    `PROJECT_ID.cymbal.customer_review` AS t
    WHERE
    t.rating < 3
    ),
    STRUCT(
    "issue_category STRING" AS output_schema,
    8192 AS max_output_tokens)
    )
    )
    ```

    > `PROJECT_ID`: 현재 프로젝트 명


### **고객 리뷰 분석을 통한 개선점 데이터 보강**

1. 위 과정에서 생성한 모델을 사용합니다.  
2. 고객 리뷰를 참고하여 개선 방안과 선호할 만한 특성을 분석한 텍스트를 생성합니다. 이때, 아래 코드에서 `PROJECT_ID` 를 설정하여 실행합니다.

    ```sql
    SELECT
    generated_output.customer_id,
    generated_output.customer_review_id,
    generated_output.ml_generate_text_result.candidates[0].content.parts[0].text AS improvement_points
    FROM
    ML.GENERATE_TEXT(
    MODEL `PROJECT_ID.cymbal.gemini_model`,
    (
      SELECT
        customer_id,
        customer_review_id,
    CONCAT(
    '''
    다음 리뷰를 참고해서 고객이 불만족한 부분을 개선할 수 있는 구체적인 방안 1개와 고객이 좋아할 법한 상품 특성을 1개 제시해줘 양식은 다음과 같아. 양식을 무조건 지켜서 작성해줘.
    개선방안 :
    고객이 좋아할 법한 특성 :
    참고할 리뷰내용 :
    ''', review_text) AS prompt
    FROM `PROJECT_ID.cymbal.customer_review`
    WHERE rating < 3),
    STRUCT(
    0.2 AS temperature,
    2048 AS max_output_tokens )
    ) AS generated_output
    ```

    > `PROJECT_ID`: 현재 프로젝트 명
    
    해당 결과는 다음과 같이 출력됩니다.

    <img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task4_img_025.png" alt="task4_img_025.png" />

### [CHALLENGE]  
위 쿼리 결과로 `improv_table` 테이블을 생성합니다. 이때 데이터가 보강되는 컬럼명은 `improvement_points`로 지정합니다.  

목표를 확인하려면 **진행 상황 확인을 클릭**하세요.
<ql-activity-tracking step=14 text="체크">
Check if improv\_table exists
</ql-activity-tracking>

### 탐색 데이터 병합  

1. `cgenf_table`과 위 Challenge에서 생성한 `improv_table`을 조인하여 보강된 데이터를 하나로 합칩니다. `PROJECT_ID`를 수정한 후 진행합니다.

    ```sql
    SELECT
      t1.*,
      t2.issue_category
    FROM `PROJECT_ID.cymbal.cgenf_table` as t2
    JOIN `PROJECT_ID.cymbal.improv_table` as t1 
    ON t1.customer_review_id = t2.customer_review_id
    ```

    > `PROJECT_ID`: 현재 프로젝트 명

2. 쿼리 결과를 저장합니다. **Save results**를 클릭하고 **CSV** 형식을 선택한 후 로컬환경에 저장합니다. 또는 아래 CHALLENGE의 힌트에서 안내하는 저장방식을 응용할 수 있습니다.  
    <img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task4_img_026.png" alt="task4_img_026.png" />  

3. 저장한 파일의 이름을 `customer_review_enf_result.csv`로 수정합니다.

### [CHALLENGE]  

제품 테이블에 장점, 단점, 특성을 정의하여 데이터를 보강합니다. 결과를 `product_enf_result.csv`로 저장합니다. 프로젝트 ID로 생성된 버킷에 `task4_result` 폴더를 만들고, 해당 폴더에 `product_enf_result.csv`와 앞서 생성한 `customer_review_enf_result.csv`를 함께 저장합니다.  

**힌트:** 위에서 학습한 `AI.GENERATE_TABLE` 또는 `ML.GENERATE_TEXT`를 활용할 수 있습니다.  

**힌트:** 결과 저장 방법은 위 실습 방법을 따르거나, 다음 방식을 활용할 수 있습니다.  

1. 쿼리 결과 창에서 **Save results**를 클릭하고 **Cloud Storage**를 선택합니다.  
    <img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task4_img_027.png" alt="task4_img_027.png" />  

2. Challenge 조건에 맞춰 **Browse**를 클릭하고 경로와 파일명을 설정하여 저장합니다.  
    <img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task4_img_028.png" alt="task4_img_028.png" />  


목표를 확인하려면 **진행 상황 확인을 클릭**하세요.
<ql-activity-tracking step=15>
`customer_review_enf_result` 파일이 해당 버킷 경로에 존재하는지 확인
</ql-activity-tracking>



## Task 5: 상품 추천 모델 생성 및 활용


#### Overview

탐색적 데이터 분석 결과를 바탕으로 데이터 사이언티스트 에이전트를 활용해 상품 추천 모델을 생성하고 활용합니다. 다양한 프롬프트와 데이터를 시도하여 자신만의 모델을 만들어 봅니다.

#### Objective

* 데이터 사이언티스트 에이전트를 활용한 모델 생성 및 성능 평가  
* 모델을 활용한 불만족 고객 대상 추천 상품 생성 
* Google Cloud Storage에 Notebook 파일 저장

### 노트북 생성하기

먼저 이 작업을 위한 Notebook을 BigQuery Studio에 생성합니다.  

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task5_img_001.png" alt="task5_img_001.png" />

1. BigQuery Studio에서 **Notebook**을 클릭합니다.  
2. **Region**을 **us-central1**로 설정 후 **Select** 버튼을 클릭합니다.
    <img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task5_img_017.png" alt="task5_img_017.png" />  
3. Notebook이 생성되면 **Connect** 버튼을 클릭하여 런타임을 연결합니다.  
4. 인증 팝업창이 나타나면 **Open** 버튼을 클릭합니다.  
    <img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task5_img_018.png" alt="task5_img_018.png" />  
5. 나타나는 qwiklabs.net 계정을 선택합니다. 

### TASK 4에서 생성한 파일 추가하기

1. Notebook 콘솔 왼쪽 하단의 **Terminal** 버튼을 클릭합니다.  
    <img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task5_img_002.png" alt="task5_img_002.png" />  

2. 터미널 창에 다음 명령어를 입력합니다. `your-bucket-name`은 본인 프로젝트의 버킷 이름으로 수정합니다.
    
    ```bash
    gsutil cp -r gs://YOUR_BUCKET/ .
    ```
    
    <img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task5_img_003.png" alt="task5_img_003.png" />


3. 아래 이미지와 같이 경로에 파일이 추가되었는지 확인합니다.   
    <img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task5_img_004.png" alt="task5_img_004.png" />  

### 데이터 사이언스 에이전트 활용하기

1. Notebook 우측 상단의 **Gemini** 버튼을 클릭합니다. 프롬프트 창이 나타나면, 추가할 파일에 커서를 올렸을 때 표시되는 Gemini 아이콘을 클릭합니다. 이 예제에서는 다음 파일을 추가합니다.
   * bq_data/alchemy_data1.csv
   * task4_result/customer_review_enf_result.csv
   * task4_result/product_enf_result.csv
    
    <img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task5_img_005.png" alt="task5_img_005.png" />
    
    **참고:**
    * 아래 이미지의 1번 버튼(패널로 이동)을 클릭하면 채팅 대화상자를 Notebook 외부의 별도 패널로 이동할 수 있습니다.
    * 파일을 직접 업로드하려면 아래 이미지의 2번 버튼을 클릭합니다.
    
    <img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task5_img_006.png" alt="task5_img_006.png" />


2. 채팅 대화상자에 프롬프트를 입력하고 **보내기**를 클릭합니다. 프롬프트 아이디어는 [데이터 과학 에이전트 기능](https://cloud.google.com/colab/docs/use-data-science-agent?hl=ko#capabilities)과 [샘플 프롬프트](https://cloud.google.com/colab/docs/use-data-science-agent?hl=ko#sample-prompts)를 참고합니다.

    | **Note:** 예를 들어 '업로드한 데이터를 분석하고 Matrix Factorization 모델을 생성하여 평가해 줘. 그리고 불만족 고객이 만족할 만한 상품을 추천하여 테이블 형태로 만들어 줘'라고 입력할 수 있습니다. 모델 설명은 다음 [링크](https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-create-matrix-factorization)를 참조합니다. |
    | :---- |
    
    <img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task5_img_007.png" alt="task5_img_007.png" />

3. Gemini가 프롬프트에 응답합니다. 응답에는 실행 가능한 코드 스니펫, 프로젝트 조언, 다음 단계, 데이터 또는 코드 문제에 대한 정보가 포함될 수 있습니다.
   응답을 평가한 후 다음 작업을 수행할 수 있습니다.
   * **Accept & Run**을 클릭하여 Notebook에 코드를 추가하고 실행합니다.
   * **Cancel**을 선택하여 추천 코드를 삭제합니다.
   * 필요에 따라 후속 질문을 하며 작업을 계속합니다.
   
    | **Note:** 각 단계마다 **Run** 을 클릭하여 작업을 진행할 수 있도록합니다. 현재 데이터 사이언스 에이전트는 초기단계로 작업 실행 시 오류가 발생하는 경우 자동으로 오류 해결을 시도하지만 해결에 실패할 수 있습니다. 이땐, 다시 동작을 시도하고 후에도 실패시 프롬프트를 수정하는 방법을 사용합니다. |
    | :---- |
    
    <img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task5_img_008.png" alt="task5_img_008.png" />


### Notebook 파일을 GCS에 저장하기

1. **File > Download > Download .ipynb**를 클릭하여 다운로드합니다.  
    <img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task5_img_009.png" alt="task5_img_009.png" />

2. Google Cloud 콘솔의 **탐색 메뉴**(☰)에서 **Cloud Storage > Buckets**를 선택합니다.  
    <img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task5_img_011.png" alt="task5_img_011.png" />

3. 실습 환경에서 제공된 버킷 이름을 클릭하고 `task5` 폴더로 진입합니다.    
4. **Upload > Upload files**를 클릭하여 Notebook 파일을 업로드합니다. 이때 파일명은 `task5_result.ipynb`로 지정합니다.  
    <img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task5_img_010.png" alt="task5_img_010.png" />  



### [CHALLENGE]  

위에서 생성한 상품 추천 테이블을 `product_recommendations`라는 이름으로 생성합니다. 이때 고객 ID는 `customer_id`, 추천 상품은 `recommended_products`라는 컬럼명을 가져야 합니다.  


목표를 확인하려면 **진행 상황 확인을 클릭**하세요.
<ql-activity-tracking step=16 text=체크>
`task5_result` 파일을 버킷에 업로드했는지 확인 
</ql-activity-tracking>

## Task 6: 멀티 소스 데이터를 활용한 고객 리타겟팅

#### Overview

이 작업에서는 이전 단계들과 **독립적으로 진행되는 작업**으로, 이탈 징후를 보이는 '부정적 segment 고객(negative segment customer)'데이터를 추가하여 해당 데이터에 맞춤화된 추천 이메일을 자동으로 발송하는 파이프라인을 **BQ의 연속 쿼리 기능**을 통해 완성합니다.

BigQuery 연속 쿼리(CQ)가 실시간으로 추가되는 데이터를 감지하고, Gemini가 추가되는 데이터를 이해하여 최종 이메일을 생성한 뒤, **Cloud Pub/sub**을 통해 **Application Integration**으로 이메일을 발송하도록 합니다.

<!-- <div><ql-infobox> -->


#### Objective

이 실습에서는 다음 방법을 배웁니다:

* BigQuery ML 원격 모델(Gemini 2.0 Flash) 생성 및 구성하기
* 사용자 지정 서비스 계정에 BigQuery 및 Pub/Sub 리소스 접근 권한 부여하기
* Application Integration 트리거 생성 및 구성하기
* Gemini를 사용하여 이메일 텍스트를 생성하는 연속 쿼리를 BigQuery에서 생성하기
* 연속 쿼리를 테스트하기 위해 결합된 데이터, negative_customer_segment_products 에 데이터 추가

> Qwiklab의 student 계정에는 아웃바운드 이메일 전송이 제한됩니다. 예상되는 이메일 시나리오를 스크린샷으로 제공합니다. 


### 1. BigQuery ML 원격 모델 생성 및 구성

이 작업을 위해 continuous_queries라는 BigQuery 데이터세트와 negative_cutomer_segment_products라는 새로운 값을 인서트할 빈 테이블을 포함한 여러 리소스가 미리 생성되어 있습니다.

이 작업에서는 워크플로우를 위한 개인화된 이메일 콘텐츠를 생성하기 위해 엔드포인트로 Gemini 2.0 Flash 를 사용하는 BigQuery ML 원격 모델을 포함한 추가 BigQuery 리소스를 생성하고 구성합니다.

### 1.1 BigQuery 원격 Connection 생성

1. Google Cloud 콘솔에서 **Navigation menu** &gt; **BigQuery**를 클릭합니다.
2. **Explorer** 창에서 **+ Add Data**를 클릭한 다음, **Vertex AI**를 검색합니다. 결과에서 **Vertex AI**를 클릭하고 뜨는 **Bigquery Federation**을 클릭합니다.
3. **Connection type**에서 Vertex AI remote models, remote functions and BigLake (Cloud Resource)를 선택합니다.
4. **Connection ID**에 **continuous-queries-connection**을 입력합니다.
5. **Location type**에서 **Region** &gt; **us-central1**을 선택합니다.
6. **Create connection**을 클릭한 다음, **Go to connection**을 클릭합니다 

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task6_gotoconnection.png" alt="task6_gotoconnection.png"  width="741.50" />

7. **Connection info** 페이지에서 다음 섹션에서 사용할 **Service account ID**를 복사합니다.  
 예: bqcx-1054723899402-whbp@gcp-sa-bigquery-condel.iam.gserviceaccount.com


### 1.2 BigQuery 서비스 계정에 Vertex AI용 IAM 역할 부여

1. Google Cloud 콘솔의 **Navigation menu**에서 **IAM & Admin** &gt; **IAM**을 선택합니다.
2. **Grant access**를 클릭합니다.
3. **New principals**에 이전 섹션에서 복사한 서비스 계정 ID를 입력합니다   
(예: bqcx-1054723899402-whbp@gcp-sa-bigquery-condel.iam.gserviceaccount.com).
4. **Select a role**에서 **Vertex AI** &gt; **Vertex AI User**를 선택합니다.
5. **Save**를 클릭합니다.

##### 1.3 BigQuery ML 원격 모델 생성

1. Google Cloud 콘솔에서 **Navigation menu**() &gt; **BigQuery**를 클릭합니다.
2. **Untitled query**를 클릭하여 빈 쿼리 창에 액세스합니다.
3. BigQuery ML 모델을 생성하기 위해 다음 쿼리를 복사하여 붙여넣고, **ProjectID**를 수정합니다.
4. **Run**을 클릭하여 실행합니다.

> Region의 경우, 본 실습에서는 us-central1으로 제약적으로 통일하고 있습니다. 이후 실제 환경에서 활용하실때는 원하는 지역으로 변경이 가능합니다.

```sql
CREATE MODEL `Project ID.continuous_queries.gemini_2_0_flash`
REMOTE WITH CONNECTION `us-central1.continuous-queries-connection`
OPTIONS(endpoint = 'gemini-2.0-flash');
```

**참고:** 서비스 계정 권한(이전 섹션에서 할당함)과 관련된 오류가 발생하면 몇 분 정도 기다린 후 쿼리를 다시 실행하세요.

#### 2 사용자 지정 서비스 계정에 BigQuery 및 Pub/Sub 리소스 접근 권한 부여

이 작업을 위해 recapture_customer라는 Pub/Sub 토픽과 bq-continuous-query-sa@Project ID.iam.gserviceaccount.com이라는 사용자 지정 서비스 계정을 포함한 여러 리소스가 미리 생성되어 있습니다.

* 이 계정명을 복사해서 이후 단계에서 활용해주세요.

이 작업에서는 이후 작업에서 개인화된 이메일을 생성하고 보내는 데 사용될 BigQuery 데이터세트, 원격 모델 및 Pub/Sub 토픽에 대한 접근 권한을 사용자 지정 서비스 계정에 부여합니다.

### 2.1 사용자 지정 서비스 계정에 원격 모델 접근 권한 부여

1. Google Cloud 콘솔에서 **Navigation menu**() &gt; **BigQuery**를 클릭합니다.
2. **Class Explorer** 창에서 **Project ID** 옆의 화살표를 확장합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task1_explorer.png" alt="task1_explorer.png"  width="541.50" />

3. **connections**를 확장하고, **us-central1.continuous-queries-connection**을 클릭합니다.
4. **Connection info** 페이지에서 **Share**를 클릭합니다.
5. **Add principal**을 클릭합니다.
6. **New principals**에 사용자 지정 서비스 계정 ID를 입력합니다:  
bq-continuous-query-sa@Project ID.iam.gserviceaccount.com
7. **Select a role**에서 **BigQuery** &gt; **BigQuery Connection User**를 선택합니다.
8. **Save**를 클릭한 다음, **Close**를 클릭합니다

### 2.2 사용자 지정 서비스 계정에 BigQuery 데이터세트 접근 권한 부여

1. **Explorer** 창에서 고객 리뷰 테이블을 포함하는 데이터세트의 이름인 continuous_queries를 클릭합니다.
2. **Dataset info** 페이지에서 **Share**을 클릭하고 **Manage Permission**를 선택합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task6_manage_permission.png" alt="task6_manage_permission.png"  width="541.50" />

3. **Add principal**을 클릭합니다.

4. **New principals**에 사용자 지정 서비스 계정 ID를 입력합니다: bq-continuous-query-sa@Project ID.iam.gserviceaccount.com
5. **Select a role**에서 **BigQuery** &gt; **BigQuery Data Editor**를 선택합니다.
6. **Save**를 클릭한 다음, **Close**를 클릭합니다.

### 2.3 사용자 지정 서비스 계정에 Pub/Sub Viewer 및 Pub/Sub Publisher 역할 부여

1. Google Cloud 콘솔에서 **Navigation menu**() &gt; **Pub/Sub**을 검색하고 클릭합니다.
2. recapture_customer 행에서 **More Actions**(세로 점 3개)를 클릭하고, **View permissions**를 선택합니다.
3. **Add principal**을 클릭합니다.
4. **New principals**에 사용자 지정 서비스 계정 ID를 입력합니다  
 bq-continuous-query-sa@Project ID.iam.gserviceaccount.com
5. **Select a role**에서 **Pub/Sub** &gt; **Pub/Sub Viewer**를 선택합니다.
6. **Add another role**을 클릭합니다.
7. **Select a role**에서 **Pub/Sub** &gt; **Pub/Sub Publisher**를 선택합니다.
8. **Save**를 클릭합니다.

목표를 확인하려면 **진행 상황 확인을 클릭**하세요.
<ql-activity-tracking step=17>
Check Pub/Sub role
</ql-activity-tracking>

### 3. Application Integration 트리거 생성 및 구성

Application Integration은 Google Cloud의 iPaaS(Integration-Platform-as-a-Service) 솔루션으로, 특정 비즈니스 운영을 지원하기 위해 통합되어야 하는 여러 애플리케이션과 데이터를 연결하고 관리하는 도구 세트를 제공합니다.

트리거(trigger)는 Application Integration에서 작업 또는 작업 시퀀스를 시작하는 외부 이벤트입니다. 이 단계에서는 Pub/Sub 토픽의 이벤트를 기반으로 하는 Pub/Sub 트리거를 사용합니다. 트리거는 통합의 진입점이라고 생각할 수 있으며, 트리거에 연결된 이벤트는 트리거와 관련된 작업이 실행되도록 합니다.

이 작업에서는 Pub/Sub 토픽으로 새 메시지가 전송될 때 통합을 실행하는 Application Integration 트리거를 생성하고 구성합니다.


### 3.1 Pub/Sub 트리거 생성

1. Google Cloud 콘솔 검색창(페이지 상단)에 **Application Integration**을 입력한 다음, 결과 목록에서 **Application Integration**을 클릭합니다.  

2. **Get started with Application Integration** 페이지의 **Region**에서 **us-central1**을 선택합니다.  

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task6_location.png" alt="task6_location.png"  width="541.50" />

3. **Quick setup**을 클릭하여 필요한 API를 활성화합니다.  

4. **Create integration**을 클릭하고, 통합에 `recommend-customer-products-integration` 이름을 지정합니다.이름 외의 설정들은 디폴트 설정을 유지합니다.  

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task6_create_integration.png" alt="task6_create_integration.png"  width="541.50" />


5. **CREATE**를 클릭합니다.  

6. `recommend-customer-products-integration` 페이지에서 **Triggers**(페이지 상단)를 클릭합니다.  

7. **Cloud Pub/Sub**을 선택하고 캔버스를 클릭하여 Pub/Sub 트리거를 추가합니다.  

8. 트리거 세부 정보 창의 **Trigger Input &gt; Pub/Sub topic**에 미리 생성된 Pub/Sub 토픽 경로를 추가합니다  
 
 projects/Project ID/topics/recapture_customer

9. **Service account**에서 사용자 지정 서비스 계정 ID를 선택합니다  
  bq-continuous-query-sa@Project ID.iam.gserviceaccount.com

> - 목록에 보이지 않으면 **Refresh list**를 클릭하세요.  
> - **Grant the necessary roles**라는 경고가 표시되면 **Grant**를 클릭하세요.  

### 3.2 Pub/Sub 트리거를 위한 데이터 매핑 변수 구성

1. 캔버스 상단에서 **Tasks**(Triggers 옆)를 클릭합니다.  

2. 검색창에 Data Mapping을 입력합니다.  

3. 결과에서 **Data Mapping**을 선택하고 캔버스를 클릭하여 **Cloud Pub/Sub Trigger** 아래에 데이터 매핑 작업을 추가합니다.  

4. **Cloud Pub/Sub Trigger**의 하단 연결점을 클릭하고 커서를 드래그하여 **Data Mapping**의 상단 연결점에 연결합니다.  

* 이제 **Cloud Pub/Sub Trigger** 하단에서 **Data Mapping** 상단으로 흐르는 화살표가 있어야 합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task6_applicationintegration1.png" alt="task6_applicationintegration1.png"  width="541.50" />

5. 캔버스에서 **Data Mapping** 항목을 클릭하고, **Open Data Mapping Editor**를 클릭합니다.

6. 다음 단계에서는 각각 `CloudPubSubMessage.data` 유형의 입력 변수 네 개를 만듭니다.

- **[변수 1] message_output**

1. **Input** 아래에서 **Variable or Value**를 클릭합니다.
2. **Variable**을 선택한 다음, **CloudPubSubMessage.data**를 선택합니다. **Save**를 클릭합니다.
3. **Output** 아래에서 **Create a new one**을 클릭합니다.
4. **Name**에 message_output을 입력합니다.
5. **Variable type**에서 **Output from integration**을 선택합니다.
6. **Data type**에서 **String**을 선택합니다.
7. **Blank default value means**에서 **Empty string**을 활성화합니다.
8. **Create**를 클릭합니다.
<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task6_applicationintegration2.png" alt="task6_applicationintegration2.png"  width="541.50" />

- **[변수 2] customer_message** 
방금 Input에 함수가 포함되지 않은 변수 하나를 만들었습니다. 이제 Input에 두 개의 함수가 포함된 다른 변수를 만듭니다.

1. **Input** 아래에서 **Variable or Value**를 클릭합니다.
2. **Variable**을 선택한 다음, **CloudPubSubMessage.data**를 선택합니다. **Save**를 클릭합니다.
3. 두 번째 변수 옆의 **Add a function**(+ 아이콘)을 클릭하고, **TO_JSON() -&gt; JSON**을 선택합니다.
4. 두 번째 변수에 대해 **Add a function**(+ 아이콘)을 다시 클릭하고, **GET_PROPERTY(String) -&gt; JSON**을 선택합니다.
5. **.GET_PROPERTY** 옆에서 **Variable or Value**를 클릭합니다.
6. **Value**를 선택하고 customer_message를 입력합니다.
<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task6_value.png" alt="task6_value.png"  width="541.50" />

7. 이 변수와 동일한 행에서, **Output** 아래에서 **Create a new one**을 클릭합니다.
8. **Name**에 customer_message를 입력합니다.
9. **Variable type**에서 **Output from integration**을 선택합니다.
10. **Data type**에서 **String**을 선택합니다. **참고:** JSON 함수가 추가되었기 때문에 기본값은 JSON이므로, 데이터 유형을 지침대로 **String**으로 변경해야 합니다.
11. **Blank default value means**에서 **Empty string**을 활성화합니다.
12. **Create**를 클릭합니다.
<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task6_variable.png" alt="task6_variable.png"  width="541.50" />

- **[변수 3 및 4] customer_email 및 customer_name** 
이전 섹션의 1~12단계를 반복하여 다음 정보를 사용하여 두 개의 변수를 더 만듭니다:


| GET_PROPERTY()의 값 | 출력 이름 |
| --- | --- |
| customer_email | customer_email |
| customer_name | customer_name |


<div><ql-infobox>
참고: JSON 함수가 추가되었기 때문에 Output의 기본 데이터 유형은 JSON이므로, 이 두 변수 모두에 대해 Output의 데이터 유형을 String으로 변경해야 합니다.
</ql-infobox></div>

이제 이 Application Integration 트리거에 대해 message_output, customer_message, customer_email, customer_name 네 개의 데이터 매핑 변수가 구성되었습니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/22b44f7f0dfa1df3.png" alt="22b44f7f0dfa1df3.png"  width="624.00" />

### 3.3 이메일 보내기 작업을 위한 OAuth Client 생성

클라이언트 ID는 Google OAuth 서버에서 단일 애플리케이션을 식별하는 데 사용됩니다. 애플리케이션이 여러 플랫폼에서 실행되는 경우 각각 고유한 클라이언트 ID가 필요합니다. 애플리케이션에서 OAuth 2.0을 사용하려면 OAuth 2.0 액세스 토큰을 요청할 때 사용하는 OAuth 2.0 클라이언트 ID가 필요합니다.

OAuth 2.0 클라이언트 ID를 생성하려면 다음 단계를 수행하세요:

1. Google Cloud 콘솔에서 **API & Services > credentials**로 이동합니다.
[credentials 페이지로 이동](https://console.cloud.google.com/apis/credentials)

2. **+ Create Credentials**를 클릭하고 사용 가능한 옵션 목록에서 **OAuth Client ID**를 선택합니다.
OAuth Client ID 만들기 페이지가 나타납니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task6_create_oauth_client_id1.png" alt="task6_oauth_client_id.png"  width="541.50" />

3. **Application type**: 드롭다운 목록에서 **Web application**을 선택합니다.
**Name**: oauth-client로 입력합니다.
4. **Authorized redirect URIs**에서 **+ Add URI**를 클릭하고 다음을 입력합니다:
```
 https://console.cloud.google.com/integrations/callback/locations/us-central1
```

5. **Create**를 클릭합니다.

OAuth 2.0 클라이언트 ID가 성공적으로 생성되었습니다.

6. Download JSON 버튼을 클릭하여 JSON 파일을 다운로드합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task6_download_json1.png" alt="task6_download_json.png"  width="541.50" />

### 3.3 이메일 보내기 작업 추가

1. 화면 상단의 **Data Mapping Task Editor** 옆에 있는 뒤로 가기 화살표(&lt;-)를 클릭하여 캔버스로 돌아갑니다.
2. 브라우저 탭을 복제합니다 (현재 탭에서 마우스 오른쪽 버튼을 클릭하고 **Duplicate(복제)** 선택).
3. Google Cloud Console에서 페이지 상단의 검색창에 **Integration Connectors**를 입력한 다음, 결과 목록에서  Connections(연결) 를 클릭합니다.
4.  Create New(새로 만들기) 를 클릭하여 새 연결을 생성합니다.
5.  Region 으로 **us-central1** 을 선택하고  Next(다음) 를 클릭합니다. 
6. Connector 드롭다운에서 **Gmail**을 선택합니다.
7.  Connection Name(연결 이름) 에 **send-email**을 입력한 다음,  Next(다음) 를 클릭합니다.
8.  Authentication(인증) 에서 **OAuth 2.0 - Authorization code** 를 선택합니다.
9.  **Client ID**로 이전 단계에서 저장한 JSON의 Client ID를 입력합니다.
10. **Client Secret**로 **Create new secret**버튼을 클릭한 후, 이전 단계에서 저장한 JSON에서 Client Secret을 복사하여 입력합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task6_create_new_secret1.png" alt=".png"  width="541.50" />

11. **Scopes(범위)**로 [**https://mail.google.com/**](https://mail.google.com/)을 입력합니다.
12. **Create(만들기)** 버튼을 클릭합니다.
13. 노란색 박스의 권한에 대한 경고창이 뜨는경우 **Grant Access(승인 권한 부여)** 버튼을 클릭하여 적합한 권한을 부여합니다.

> 커넥터가 처음 프로비저닝되는 경우 연결 생성에 5~10분이 소요될 수 있습니다.

14. 연결을 생성한 후, **Authorization Required(승인 필요)** 상태를 클릭한 다음  Authorize(승인)를 클릭하고 학생 ID(Student ID)를 사용하여 로그인합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task6_auth_required.png" alt=".png"  width="541.50" />

15.  **Continue(계속)** 를 클릭한 다음 페이지를 새로고침하여 상태가 녹색 체크 표시와 함께  Active(활성)으로 변경되는 것을 확인합니다.

브라우저의 Application Integration 탭으로 돌아갑니다.

16. 캔버스 상단에서 **Triggers(트리거)** 옆의  Tasks(태스크) 를 클릭합니다.
17. 검색창에 **Gmail**을 입력합니다.
18. 결과에서 **Gmail**을 선택하고 캔버스를 클릭하여 **Data Mappings(데이터 매핑)** 아래에 Gmail 태스크를 추가합니다.
19. **Data Mapping**의 하단 연결점을 클릭하고 커서를 드래그하여 **Gmail**의 상단 연결점에 연결합니다.

이제 **Cloud Pub/Sub Trigger**와 **Data Mapping**을 연결하는 첫 번째 화살표 외에, **Data Mapping** 하단에서 **Gmail** 상단으로 흐르는 두 번째 화살표가 생겼습니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/6848190bb9b4107c.png" alt="6848190bb9b4107c.png"  width="541.50" />


20. 캔버스에서 **Gmail** 항목을 클릭하여 세부 정보를 확인합니다.
21.  **Configure Connector** 를 클릭하고, Region(리전)으로  [리전 이름]을 선택한 다음 connection(연결) 드롭다운에서 **send-email**을 선택하고 Next(다음) 를 클릭합니다.
22. **Set entities/actions**에서 **gmail.users.drafts.send**를 선택한 다음 Done(완료)을 클릭합니다.
23. recommend-customer-products-integration  페이지의 오른쪽 상단에서 Publish(게시)를 클릭합니다.
<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task6_justpublish.png" alt="task6_justpublish.png"  width="541.50" />

위와 같은 Autogenerate integration description 창이 뜨는 경우, `NO, JUST PUBLISH`를 클릭합니다.

목표를 확인하려면 **진행 상황 확인을 클릭**하세요.
<ql-activity-tracking step=18>
Create and publish Application Integration
</ql-activity-tracking>

### 4. BigQuery에서 Gemini로 이메일 텍스트를 생성하는 연속 쿼리(continuous query) 만들기

이전 태스크에서, BigQuery ML 원격 모델 및 Pub/Sub용 Application Integration 트리거와 같이 통합에 필요한 다양한 구성 요소를 생성하고 구성했습니다. 이 태스크에서는 워크플로우의 마지막 조각을 생성합니다. 즉, 새로 추천되는 제품이 있는지 BigQuery 테이블을 모니터링하고, 해당 고객을 위한 맞춤형 프로모션 이메일을 생성하도록 Gemini에 요청을 보낸 다음, 개인화된 이메일 콘텐츠를 Pub/Sub 주제(topic)에 작성하는 연속 쿼리(continuous query)를 생성합니다.

### 4.1 BigQuery Enterprise reservation 만들기

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task6_capacitymanagement.png" alt="task6_capacitymanagement.png"  width="300.50" />

* Google Cloud 콘솔에서 **Navigation 메뉴**() &gt; **BigQuery** &gt; **Capacity Management**를 클릭합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task6_reservation_button.png" alt="task6_email.png"  width="541.50" />


* **Create reservation**을 클릭합니다.
* 예약 이름(reservation name)에 다음을 입력합니다: bq-continuous-queries-reservation
* 위치(Location)에서 **us-central1**을 선택합니다.
* 버전(Edition)에서 **Enterprise**를 선택합니다.
* Max reservation size selector에서 **Extra Small (50 slots)**을 선택합니다.
* Baseline slots에 **0**을 입력합니다.
* 그 외의 항목은 디폴트 설정을 유지합니다.
* **저장**을 클릭합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task6_create_reservation.png" alt="task6_reservation.png"  width="541.50" />


### 4.2 Assignment 만들기

예약이 생성된 후, slot reservation table에서 bq-continuous-queries-reservation 이름의 예약 행을 찾습니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task6_create_assignment.png" alt="task6_assignment.png"  width="541.50" />

* **Actions** (세로 점 3개) 아래에서 **Reservation actions**을 클릭하고 **Create assignment**를 선택합니다.
* **Select an Organization, folder or project**에서 를 클릭하고 이 프로젝트(**Project ID**)를 선택합니다.
* Job type으로 Continuous을 선택합니다.
* **Create**를 클릭합니다.
* bq-continuous-queries-reservation 예약 옆의 화살표를 확장하여 projects/Project ID로 표시되는 새 할당을 볼 수 있습니다.

### 4.3 Bigquery에서 ML의 결과물 미리보기
핵심 SELECT 쿼리를 실행하여, Pub/Sub으로 전송될 최종 결과물을 화면에서 직접 확인합니다.

* 다음 쿼리를 복사하여 연속 쿼리를 생성합니다. **아직 실행(run)을 클릭하지 마세요.**

```sql
--- 미리보기용 쿼리
--- PROJECT_ID를 실제 값으로 변경해주세요.
SELECT
   TO_JSON_STRING(
     STRUCT(
       customer_name AS customer_name,
       customer_email AS customer_email,
       REGEXP_REPLACE(REGEXP_EXTRACT(ml_generate_text_llm_result,r"(?im)\<html>(?s:.)*\</html>"), r"(?i)\[your name\]", "Your friends at AI Megastore") AS customer_message
     )
   ) AS pubsub_message
 FROM ML.GENERATE_TEXT(
   MODEL `PROJECT_ID.continuous_queries.gemini_2_0_flash`,  
   (
     SELECT
       ncs.customer_name,
       ncs.customer_email,
       CONCAT(
         "Write a personalized retention email in HTML format to customer ", ncs.customer_name,
         ", who is in our '", ncs.segment, "' segment. ",
         "We understand they were unhappy with product_id '", ncs.top_products, "'. ",
         "Apologize for the experience and offer them these specific recommendations as an alternative: ",
         ncs.recommended_products,           ". Keep the tone supportive and encouraging."
       ) AS prompt
     FROM
      APPENDS(TABLE `continuous_queries.negative_customer_segment_products`,
               CURRENT_TIMESTAMP() - INTERVAL 10 MINUTE) as ncs
   ),
   STRUCT( 1024 AS max_output_tokens,
     0.2 AS temperature,
     1 AS candidate_count,
     TRUE AS flatten_json_output)
 )
```


1. 쿼리 창 위에서 **추가 작업**(세로 점 3개) &gt; **더보기**(톱니바퀴 아이콘)를 클릭하고 **쿼리 모드 선택**에서 **연속 쿼리**를 선택합니다.
2. 메시지가 표시되면 **확인**을 클릭합니다.
3. 다시 **추가 작업**(세로 점 3개) &gt; **더보기**(톱니바퀴 아이콘)를 클릭하고 **쿼리 설정**을 선택합니다.
4. **연속 쿼리** 아래의 **서비스 계정**에서 커스텀 서비스 계정(bq-continuous-query-sa@Project ID.iam.gserviceaccount.com)을 선택합니다.
5. **저장**을 클릭하여 쿼리 설정을 종료합니다.
6. 쿼리 창에서 **실행**을 클릭하여 연속 쿼리를 시작합니다.

* 연속 쿼리가 시작되는 데 몇 분 정도 걸릴 수 있습니다.

7. 쿼리 창 상단에 **작업이 계속 실행 중(Job running continuously)** 상태가 표시되면 다음 태스크로 진행할 수 있습니다.


8. BigQuery에서 **제목 없는 쿼리** 오른쪽에 있는 **+** 아이콘(**SQL 쿼리**)을 클릭하여 새 쿼리 창을 엽니다.
9. 다음 쿼리를 복사하여 새로운 추천 더미 데이터를 테이블에 테스트로 삽입하고, student_username을 수정한 후, **실행**을 클릭합니다.

이 쿼리를 실행하면, 쿼리 결과 창에 다음 쿼리를 통해서, Pub/Sub으로 전송될 데이터와 똑같은 형식의 JSON 문자열이 표시됩니다. 이 내용을 보고 이메일 메시지가 의도대로 잘 생성되는지 검토할 수 있습니다.

```sql
-- 'negative_customer_segment_products' 테이블에 테스트 데이터를 삽입합니다.
INSERT INTO continuous_queries.negative_customer_segment_products
(
    customer_id,
    customer_name,
    customer_email,
    segment,
    top_products,
    recommended_products
)
VALUES
(1001, 'Alice Johnson', '{student-username}', 'negative', 'Smart Thermostat, Wireless Headphones', 'Smart Lighting Kit, Portable Speaker');
```


### 4.3 BigQuery에서 continuous query 만들기

미리보기 결과가 만족스럽다면, 이제 `EXPORT DATA` 쿼리를 사용하여 실제 Continuous Query를 생성하고 실행합니다.

* BigQuery 왼쪽 메뉴에서 **Studio**를 클릭합니다.
* **Untitled Query**를 클릭하여 빈 쿼리 창에 액세스합니다.
* 다음 쿼리를 복사하여 연속 쿼리를 생성합니다. **아직 실행(run)을 클릭하지 마세요.**

```sql
EXPORT DATA
 OPTIONS (format = CLOUD_PUBSUB,
 uri = "https://pubsub.googleapis.com/projects/PROJECT_ID/topics/recapture_customer") AS (
SELECT
   TO_JSON_STRING(
     STRUCT(
       customer_name AS customer_name,
       customer_email AS customer_email, 
       REGEXP_REPLACE(REGEXP_EXTRACT(ml_generate_text_llm_result,r"(?im)\&lt;html\&gt;(?s:.)*\&lt;\/html\&gt;"), r"(?i)\[your name\]", "Your friends at AI Megastore") AS customer_message
     )
   )
 FROM ML.GENERATE_TEXT(
   MODEL `PROJECT_ID.continuous_queries.gemini_2_0_flash`,  -- 사용자의 모델 경로
   (
     SELECT
       ncs.customer_name,
       ncs.customer_email,
       CONCAT(
         "Write a personalized retention email in HTML format to customer ", ncs.customer_name, 
         ", who is in our '", ncs.segment, "' segment. ", 
         "We understand they were unhappy with product_id '", ncs.top_products, "'. ",
         "Apologize for the experience and offer them these specific recommendations as an alternative: ",
         ncs.recommended_products,           ". Keep the tone supportive and encouraging."
       ) AS prompt
     FROM
       -- Table B(negative_customer_recommended_products)에 새로 추가되는 행을 감지
      APPENDS(TABLE `continuous_queries.negative_customer_segment_products`, 
               CURRENT_TIMESTAMP() - INTERVAL 10 MINUTE ) as ncs
   ),
   STRUCT( 1024 AS max_output_tokens,
     0.2 AS temperature,
     1 AS candidate_count, 
     TRUE AS flatten_json_output)
 )
)
```

4. 쿼리 창 위에서 **추가 작업**(세로 점 3개) &gt; **더보기**(톱니바퀴 아이콘)를 클릭하고 **쿼리 모드 선택**에서 **연속 쿼리**를 선택합니다.
5. 메시지가 표시되면 **확인**을 클릭합니다.
6. 다시 **추가 작업**(세로 점 3개) &gt; **더보기**(톱니바퀴 아이콘)를 클릭하고 **쿼리 설정**을 선택합니다.
7. **연속 쿼리** 아래의 **서비스 계정**에서 커스텀 서비스 계정(bq-continuous-query-sa@Project ID.iam.gserviceaccount.com)을 선택합니다.
8. **저장**을 클릭하여 쿼리 설정을 종료합니다.
9. 쿼리 창에서 **실행**을 클릭하여 연속 쿼리를 시작합니다.

* 연속 쿼리가 시작되는 데 몇 분 정도 걸릴 수 있습니다.

10. 쿼리 창 상단에 **작업이 계속 실행 중(Job running continuously)** 상태가 표시되면 다음 태스크로 진행할 수 있습니다. 


### 5. 연속 쿼리 테스트를 위한 데이터 추가

연속 쿼리를 테스트하기 위해 테스트 데이터를 'negative_customer_segment_products' 테이블에 데이터 추가합니다

마지막 태스크에서는 negative_customer_segment_products 테이블에 일부 데이터를 추가하여, 고객에게 개인화된 이메일을 보내는 Application integration 작업을 시작함으로써 연속 쿼리를 테스트합니다.

1. BigQuery에서 **제목 없는 쿼리** 오른쪽에 있는 **+** 아이콘(**SQL 쿼리**)을 클릭하여 새 쿼리 창을 엽니다.
2. 다음 쿼리를 복사하여 새로운 추천 더미 데이터를 테이블에 테스트로 삽입하고, student_username을 수정한 후, **실행**을 클릭합니다.

```sql
-- 'negative_customer_segment_products' 테이블에 테스트 데이터를 삽입합니다.
INSERT INTO continuous_queries.negative_customer_segment_products
(
    customer_id,
    customer_name,
    customer_email,
    segment,
    top_products,
    recommended_products
)
VALUES
(1002, 'David Kim', '{stduent-username}', 'Low-Engagement', 'Coffee Grinder, Yoga Mat', 'Electric Kettle, Foam Roller');
```

결과창에 **이 문(statement)이 negative_customer_segment_products에 1개의 행을 추가했습니다**라는 메시지가 표시되면 이 태스크를 완료한 것입니다.

* 실제 이메일 전송 기능을 활용하기 위해서는 Google Cloud Platform의 일반 사용자 계정이 필요합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/task6_email.png" alt="task6_email.png"  width="541.50" />

워크플로우 구축을 완료 하면, 위와 같이 이메일이 제작됩니다.
이메일로 전송될 추천 제품 테이블에 새 행을 삽입함으로써, negative sentiment을 가진 사용자에 대한 맞춤형 이메일을 보내는 워크플로우를 구축했습니다.  