# [KR] BigQuery Hackathon


## **Overview**


본 챌린지 랩은 Google Cloud의 데이터 분석 기능(BQML, Gemini in Bigquery 등)과 ML기능을 활용하여, 고객 데이터 분석부터 실행(action)까지 이어지는 엔드투엔드(end-to-end) 개인화 마케팅 파이프라인을 구축하는 실습 경험을 제공합니다.

팀 단위의 챌린지 랩(Team Assignments)를 위해, 하나의 챌린지 랩 시나리오에 3명의 팀원들에게 추가적으로 사용자 계정을 부여한 다음, 그에 따라 총 4명의 팀원에게 작업을 할당하게 됩니다.

## Objective

* **데이터 분석 (Analysis):** BQ와 Gemini를 함께 사용하여 이미지, 비디오, 텍스트 등 **멀티모달(multimodal) 고객 리뷰**의 감성(Sentiment)을 분석합니다.
* **세분화 및 타겟팅 (Segment & Target):** EDA를 통해 고객을 세분화(Segmentation)하고, 특히 **부정적인 피드백을 남긴 고객**을 식별하여 이들을 위한 맞춤형 프로모션 메시지를 생성합니다.
* **모델링 및 예측 (Model & Predict):** 더 나은 제품 추천 모델을 만들기 위해 추가 EDA를 진행하고 피처(feature)를 도출한 뒤, BigQuery Studio의 Data Science Agent(DSA)를 활용해 고급 추천 모델을 구축하고 예측 결과를 BigQuery 테이블로 저장합니다.
* **자동화 및 실행 (Automate & Activate):** BigQuery의 고객 경험 기반의 데이터 분석 결과를 **Application Integration**을 통해 맞춤형 이메일을 자동으로 전송하는 워크플로우를 구축하여 확보된 인사이트를 즉각적인 마케팅 활동으로 연결합니다.

## Scenario

온라인 리테일러인 Cymbal E-Commerce는 온라인 플랫폼을 강화하여 변화하는 고객 요구에 신속하게 적응하는 것을 목표로 합니다. 이를 달성하기 위해, 고객의 데이터를 분석하고, BigQuery와 Gemini를 활용해서 고객 이탈률을 최소화하는 방안을 구축하고 있습니다.

이러한 계획은 고객 피드백과 선호도를 신속하게 분석해야 할 필요성에 따라 추진되었으며, 이를 통해 Cymbal E-Commerce는 부정적인 피드백을 남긴 고객에 대한 프로모션 전략을 수립하고, 변화하는 고객의 요구 사항에 빠르게 대응할 수 있습니다. 개인화된 제품에 대한 감성 분석 기반의 제품 추천을 제공하여 온라인 쇼핑 여정을 더욱 매력적으로 만들 것입니다.

Cymbal E-Commerce는 이러한 혁신에 생성형 AI가 필수적임을 인지하고 Google Cloud를 선택했습니다. Google Cloud의 최첨단 AI 기술은 이 지능형 솔루션을 개발하고 확장하는 데 필요한 강력한 기반을 제공하며, 리테일 산업에서 Cymbal E-Commerce의 선도적인 입지를 공고히 할 것입니다.

## Your Challenge

여러분의 핵심 과제는 이 전략적 계획을 신속하게 발전시키는 것입니다. 여기에는 BigQuery를 사용하여 고객 피드백을 신속하게 분석하는 작업이 포함되며, 이는 맞춤형 캠페인 전략을 수립하고 구체화하는 데 활용됩니다. 여러분은 다양한 채널에서 이러한 캠페인을 실행해야 하며, 이때에도 BigQuery의 여러 기능을 활용하여 각 채널에 맞는 매력적인 콘텐츠를 제작해야 합니다.

캠페인 외에도, 제품 카탈로그 관리를 개선하고 캠페인을 통해 고객 이탈률을 최소화 하기 위한 추천 모델을 향상시키는 임무를 맡게 됩니다. 무엇보다 중요한 것은, BQML를 기반으로 데이터를 분석하고 철저하게 테스트해야 한다는 것입니다.

특히, 이 BQML를 사용하여 마케팅 전략을 수립하고 실행 및 테스트 프로세스 속도를 획기적으로 높일 수 있습니다.

## Task Outline

* Task 1: Analyzing Multimodal Customer Reviews for Marketing Insights
* Task 2: Segmenting Customers for Targeted Marketing  
* Task 3: Creating Tailored email message including promotions  for unsatisfied customers 
* Task 4: Additional Exploratory Data Analysis
* Task 5: Enhancing Product Recommendations ML model
* Task 6: Sending a customized email with Application Integrations

## Task Dependencies

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/2bdc11dcabc95304.png" alt="2bdc11dcabc95304.png"  width="624.00" />



## Task 1: 마케팅 인사이트를 위한 고객 리뷰 멀티모달 분석


#### **Overview**

고객 리뷰 분석 실습에 오신 것을 환영합니다! 이 작업에서는 텍스트뿐만 아니라 이미지와 동영상을 포함하는 멀티모달 고객 리뷰 데이터를 다루게 됩니다. 우리의 목표는 BigQuery와 Vertex AI의 Gemini Pro 모델의 강력한 기능을 활용하여 이 다양한 데이터를 처리하고, 그 결과로 얻은 인사이트를 통합하여 고객 경험에 대한 이해를 얻고 이를 적용 할 수 있게 하는 것입니다.

#### **Objective**

* BigQuery Studio에서 챌린지 실습 Notebook을 설정하고 실행합니다.
* 외부 테이블을 생성하여 Cloud Storage에 저장된 파일의 데이터를 직접 쿼리합니다.
* Gemini 모델을 호출하여 텍스트, 이미지, 비디오 리뷰의 콘텐츠를 분석합니다.
* 모든 분석 결과를 하나의 포괄적인 테이블로 통합합니다.
* 감성 분포와 시간 경과에 따른 추세를 시각화하여 실행 가능한 비즈니스 인사이트를 도출합니다.

#### **Setup**

이 초기 설정은 BigQuery와 Cloud Storage가 Vertex AI Gemini 모델과 통신할 수 있도록 권한을 구성하는 과정을 포함합니다.

#### **시작하기 전: Cloud Storage의 샘플 데이터 탐색**

실습 단계를 시작하기 전에 작업할 샘플 데이터를 간단히 살펴보겠습니다. Cloud Storage Bucket에서 고객 리뷰 텍스트, 이미지, 비디오에 직접 액세스하여 볼 수 있습니다. 이를 통해 멀티모달 데이터를 더 잘 이해할 수 있습니다.

1. Google Cloud 콘솔에서 **Navigation Menu**(☰)로 이동하여 **Cloud Storage &gt; Bucket**을 선택합니다.
2. 실습 환경에 제공된 Bucket 이름을 클릭합니다 (일반적으로 your-project-id-bucket, 예: qwiklabs-gcp-xx-xxxxx-bucket 형식).
3. **Bucket** 내부에서 review/ 폴더로 이동합니다.
4. 다음 항목을 찾을 수 있습니다:

* **고객 리뷰 (CSV):** customer_reviews.csv를 클릭하여 원시 텍스트 리뷰 데이터를 미리 봅니다.
* **리뷰 이미지:** images/ 폴더로 들어가 샘플 이미지 파일을 확인합니다. 개별 이미지를 클릭하여 직접 볼 수 있습니다.
* **리뷰 비디오:** videos/ 폴더로 들어가 샘플 비디오 파일을 확인합니다. 개별 비디오를 클릭하여 콘솔 내에서 재생할 수 있습니다.

실습의 분석 작업을 진행하기 전에 이 파일들을 자유롭게 둘러보며 콘텐츠에 익숙해지세요.

#### **1.1.BigQuery Cloud 리소스 연결 생성**

먼저, BigQuery가 Gemini 모델과 작동할 수 있도록 Cloud 리소스 연결을 생성합니다.

1. Google Cloud 콘솔에서 **Navigation menu**(☰)로 이동하여 **BigQuery**를 선택합니다.
2. **Explorer** 패널에서 **+ Add Data**를 클릭한 다음, Vertex AI를 입력하고 Vertex AI를 클릭 한 뒤 나오는 **BigQuery Federation**을 클릭합니다.
3. **connection ID**에 **gemini_conn**을 입력합니다.
4. **리전 유형**으로 **리전**을 선택한 다음, 드롭다운에서 **us-central1**을 선택합니다.
5. **CREATE CONNECTION**를 클릭합니다.
6. 확인 창이 나타납니다. **GO TO Connections**을 클릭합니다.
7. **Connection info** 창(us-central1.gemini_conn)에서 **Service account ID**를 찾아 텍스트 편집기에 복사합니다. 다음 단계에서 필요합니다.

<img src="images/task1_gotoconnection.png" alt="gotoconnection.png"  width="348.90" />

목표를 확인하려면 **진행 상황 확인을 클릭**하세요.
<ql-activity-tracking step=1>
Create BigQuery External Connection      
</ql-activitiy-tracking>      


#### **1.2. 서비스 계정에 IAM 역할 부여**

연결과 연관된 서비스 계정은 Vertex AI 및 Cloud Storage에 액세스할 수 있는 권한이 필요합니다.

##### **Vertex AI User / Storage Object Admin 역할 부여**

1. **Navigation menu**(☰)로 이동하여 **IAM 및 관리자 &gt; IAM**을 선택합니다.
2. **+ Grant Access**를 클릭합니다.
3. **New principals** 필드에 이전에 복사한 Service account를 붙여넣습니다.
4. **Select a role** 필드에서 **Vertex AI User** 및 **Storage Object Admin** 역할을 선택합니다.
5. **저장**을 클릭합니다.

<img src="images/task1_iam.png" alt="grant_access.png"  width="348.90" />

목표를 확인하려면 **진행 상황 확인을 클릭**하세요.
<ql-activity-tracking step=2>
  Grant permissions to SA for Connection
</ql-activity-tracking>

#### **2.1.1 Notebook 업로드**

먼저, 이 작업을 위한 Notebook을 BigQuery Studio에 업로드하겠습니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/81603d13197012ee.png" alt="81603d13197012ee.png"  width="348.90" />

1. Google Cloud 콘솔에서 **BigQuery**로 이동합니다.
2. **Explorer** 창에서 **Notebook** 옆에 있는 점 3개(⋮) 아이콘을 클릭하고 **URL에서 Notebook 업로드**를 선택합니다.
3. https://github.com/seoeunbae/da-hackerthon-instruction/blob/main/task1.ipynb 를 입력합니다.
4. Notebook이 새 탭에서 열리면 셀을 순서대로 실행할 준비가 된 것입니다.

#### **2.1.2 환경 초기화**

![alt text](images/task1_notebook1.png)


먼저 설정 쉘을 실행합니다. 이 셀은 필요한 모든 라이브러리를 가져오고, BigQuery에 대한 연결을 초기화하며, 실습 전반에 걸쳐 사용할 주요 변수(프로젝트 ID 및 GCS Bucket 경로 등)를 정의합니다.  알맞은 프로젝트 ID로 변경해주세요

그리고 다음 셀을 실행하세요. 

![alt text](images/task1_project_setting.png)

이 셀은 실행 완료 시 필요한 라이브러리를 가져오고, BigQuery 클라이언트를 초기화하며,
분석을 위한 전역 변수를 설정합니다.


#### **2.2.1 텍스트 리뷰 외부 테이블 생성**

다음으로, BigQuery 외부 테이블을 생성합니다. 이 쿼리는 Cloud Storage의 파일로 작업하는 방법으로, 소스 파일을 직접 참조하는 테이블 스키마를 정의하고 생성합니다. 

실행완료시 다음과 같은 로그와 함께 `customer_reviews_external`테이블을 생성합니다.

![alt text](images/task1_external_table.png)



#### **2.2.2 텍스트 리뷰 테이블 확인**

외부 테이블이 올바르게 생성되었는지 확인해 봅시다. 다음 셀은 처음 5개 행을 쿼리합니다.

```sql
%%bigquery

SELECT * FROM `cymbal.customer_reviews_external`
LIMIT 5
```

출력은 다음과 같습니다: 

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/16fded7203a3a48c.png" alt="16fded7203a3a48c.png"  width="624.00" />

#### **2.3.1 이미지 및 비디오용 객체 테이블 생성**

마찬가지로, 비정형 미디어 파일(이미지 및 비디오)에 대한 객체 테이블을 생성해야 합니다. 실행완료 시 아래와 같은 로그와 함께 BigQuery와 Gemini가 해당 파일에 액세스하고 분석할 수 있습니다.

![alt text](images/task1_create_img:video_external.png)


#### **2.3.2 BigQuery 객체 테이블 확인**

BigQuery 객체 테이블 확인 셀 실행 (이미지 리뷰) 출력은 다음과 같습니다: 

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/8df9945a5bc641d9.png" alt="8df9945a5bc641d9.png"  width="624.00" />

BigQuery 객체 테이블 확인 셀 실행 (비디오 리뷰) 출력은 다음과 같습니다: 

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/6bbca3a1ca6975f4.png" alt="6bbca3a1ca6975f4.png"  width="624.00" />

목표를 확인하려면 **진행 상황 확인을 클릭**하세요.
<ql-activity-tracking step=3>
Create External Review Tables and Upload Data 
</ql-activity-tracking>

#### **2.4.1 BigQuery에 Gemini 모델 생성**

이제, BigQuery 데이터세트 내에 gemini-2.0-flash 모델을 등록합니다. 이 단계를 통해 강력한 생성 모델을 SQL 쿼리에서 직접 호출할 수 있게 됩니다.


이 SQL 명령어를 실행 완료 시, BigQuery에 원격 모델을 생성하고,
이전에 설정한 연결을 통해 Gemini Flash 엔드포인트에 연결합니다.

![alt text](images/task1_create_model.png)  

목표를 확인하려면 **진행 상황 확인을 클릭**하세요.
<ql-activity-tracking step=4>
Create Gemini Model
</ql-activity-tracking>

#### **2.4.2 텍스트 키워드 및 감성 분석**

데이터와 모델이 준비되었으니 첫 번째 분석을 할 차례입니다. 아래 셀에서 Gemini 모델을 호출하여 각 텍스트 리뷰를 읽고 (주요 용어 추출 및 감정 분석) 두 가지 작업을 수행합니다.

텍스트 감정 분석 완료 시, 다음과 같은 로그와 함께 작업이 완료됩니다.

![alt text](images/task1_analyze_text.png)

#### **2.4.3 텍스트 분석 결과 확인**

결과를 검토해 봅시다. 다음 셀은 Gemini가 생성한 키워드와 감성이 포함된 두 개의 새 테이블의 처음 몇 행을 표시합니다.

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

#### **2.5.1 이미지 및 비디오 분석**

이제 멀티모달 부분입니다. Gemini에게 리뷰의 이미지와 비디오를 분석하여 각각에 대한 요약과 키워드를 생성하도록 요청할 것입니다.

실행완료 시, 다음과 같은 로그와 함께 분석이 완료됩니다.

![alt text](images/task1_analyze_img:video.png)

#### **2.5.2 이미지 및 비디오 분석 샘플 검토**

결과를 더 구체적으로 만들기 위해, 이 셀은 실제 미디어 파일을 해당 AI 생성 분석 결과 바로 아래에 표시합니다. 이를 통해 모델 출력의 품질을 시각적으로 확인 할 수 있습니다.

이미지 분석 출력 예시:

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/fa1faaa05252648c.png" alt="fa1faaa05252648c.png"  width="624.00" />

비디오 분석 출력 예시:

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/b37008336fda302a.png" alt="b37008336fda302a.png"  width="624.00" />

#### **2.6.1 통합 분석 테이블 생성**

이제 하나로 합쳐 보겠습니다. 다음 쿼리를 통해, 원본 리뷰 데이터를 모든 새로운 분석 테이블(텍스트, 이미지, 비디오)과 조인하여 하나의 포괄적인 멀티모달 결과 테이블을 생성하게 됩니다.

![alt text](images/task1_unified_table.png)

#### **2.6.2 통합 테이블 확인**

최종 통합 테이블을 살펴보겠습니다. 다음 쿼리는 처음 30개 고객 리뷰의 결과를 보여줍니다.

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

#### **2.7.1 GenAI로 sentiment 분석 시각화**

이 단계에서는 Notebook에 내장된 생성형 AI Assist를 사용하여 플롯을 생성합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/8403d1142e8f1303.png" alt="8403d1142e8f1303.png"  width="624.00" />

1. **+ code** 버튼을 클릭하여 새 코드 셀을 추가합니다.
2. 새 셀 안에서 **generate** 버튼을 클릭합니다.
3. 프롬프트 상자에 다음을 주석으로 입력합니다:

```
# multimodal_customer_reviews 테이블의 text_sentiment 분포에 대한 막대 차트를 그려줘

plot a bar chart for the distribution of text_sentiment in the multimodal_customer_reviews table
```

제안된 코드를 수락한 다음 셀을 실행하여 차트를 표시합니다. 이는 전반적인 감성 분석에 대한 빠른 개요를 제공합니다. 출력은 다음과 같습니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/a22946ea304fcf84.png" alt="a22946ea304fcf84.png"  width="362.50" />

#### **2.7.2 감성 분포 시각화 확인**

다음 코드를 통해 알맞은 횟수의 sentiment가 시각화되었는지 확인하세요.

```sql
%%bigquery
SELECT count(customer_id) as count, sentiment_json_string as sentiment FROM `cymbal.multimodal_customer_reviews`
GROUP BY sentiment_json_string
```

#### **Hands-on : GenAI로 플롯 생성하기**

이제 간단한 프롬프트를 작성하여 직접 시각화를 만들어 보겠습니다. 여러분의 과제는 생성형 AI 어시스턴트에게 새롭고 창의적인 질문을 하여 table_id_multimodal_reviews 테이블에서 숨겨진 패턴과 인사이트를 발견하는 것입니다.

아래는 여러분에게 영감을 줄 몇 가지 예시입니다. 이것들을 실행해보고 자신만의 것을 만들어 보세요!   
**참고:** 생성된 코드에서 오류가 발생하면 코드 셀을 삭제하고 다시 시도하세요.

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

> "Click Check my progress to verify the objective" 아래 각 단계 에 있는 활동 추적 버튼을 통해 확인하세요.

만약 모든 단계를 완료했음에도 활동 추적이 성공하지 않으면 나중에 다시 시도해 주세요.

이것으로 작업 1이 완료되었습니다. 멀티모달 고객 리뷰를 성공적으로 분석하고, 인사이트를 통합하고, 감성 트렌드를 시각화했습니다



## Task 2: 고객 세분화를 통한 타겟 마케팅


**개요**

Task 1에서 심층 분석을 통한 리뷰 데이터가 준비되었으므로 다음 단계인 고객 세분화를 진행합니다. 이번 태스크에서는 태스크 1에서 얻은 인사이트와 고객 인구통계 데이터를 결합합니다. 이어서 Gemini를 사용하여 각 고객 세그먼트에 대한 상세한 페르소나를 동적으로 생성하고, 이를 타겟 마케팅에 활용합니다.

이번 태스크에서는 BigQuery Studio의 **Notebook** 또는 **Data Canvas**를 활용하는 **두 가지 방법 중 하나를 선택**해 태스크를 진행합니다.

**목표**

* 멀티모달 리뷰 분석과 고객 인구통계 데이터 결합
* 연령, 성별, 충성도를 기준으로 고객 세그먼트 정의
* Gemini를 사용하여 각 세그먼트에 대한 풍부하고 상세한 페르소나 생성
* 상세한 세그먼트 인사이트와 페르소나 정의를 담은 결과 테이블 생성

**노트북 업로드**

1. BigQuery Studio 탐색기 창에서 Notebooks 옆의 점 3개(⋮) 아이콘을 클릭한 후 'URL에서 노트북 업로드(Upload notebook from URL)'를 선택합니다.
<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/instr-task2/qwiklabs/instructions/images/task2_image1.png" alt="task2_image1.png"  width="624.00" />
2. Upload from 에서 URL 선택 후 https://github.com/cheeunlim/dnpursuit_da_hackathon/blob/main/task2.ipynb를 입력합니다.
3. Region: us-central-1을 선택합니다.
4. "Upload" 버튼을 누른 후, 화면 하단의 "Go to notebook" 알림 버튼을 눌러 새로운 노트북 탭을 엽니다. 이 노트북의 셀들을 실행하여 태스크 2를 진행하겠습니다.

목표를 확인하려면 **진행 상황 확인을 클릭**하세요.
<ql-activity-tracking step=7>
Upload a Notebook on BigQuery Studio
</ql-activity-tracking>

**1. Task 2 환경 초기화**

태스크 셋업을 위해 초기 설정 셀을 실행합니다. 이 셀은 필요한 라이브러리를 가져오고, BigQuery 클라이언트를 초기화하며, 이 랩에서 사용될 주요 변수(예: PROJECT_ID, DATASET_ID)를 정의합니다.
<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/instr-task2/qwiklabs/instructions/images/task2_image2.png" alt="task2_image2.png"  width="624.00" />

이때 반드시 Project ID를 Qwiklabs 환경에서 주어진 Project ID로 변경해줍니다.

**2. 고객 데이터 EDA 및 고객 세그먼트 세분화 로직 정의**

이 단계는 Task 1의 cymbal.multimodal_customer_reviews 테이블과 cymbal.customers 테이블을 customer_id를 기준으로 조인하고, 이를 바탕으로 고객 세분화를 위한 핵심 속성을 탐색하는 과정입니다. 여러분은 이 탐색을 통해 발견된 유의미한 패턴을 기반으로 세분화 기준과 로직을 자유롭게 정의할 수 있습니다.

Task 1의 다중 모달 리뷰 분석 결과와 고객 인구통계 데이터를 결합하고, 결합된 데이터에 대한 EDA를 수행하여 고객 세분화를 위한 핵심 속성을 식별합니다. 이 인사이트를 바탕으로 다음과 같은 세그먼트 기준을 정의합니다:

age_group: 40세 미만은 'Younger_Adult', 40세 이상은 'Older_Adult'
gender_segment: gender 컬럼의 값을 대문자로 변환 (예: 'MALE', 'FEMALE')
loyalty_status: loyalty_member가 True면 'LOYAL', False이면 'NON_LOYAL'
text_sentiment: sentiment_json_string 컬럼에서 '$.sentiment'를 추출

결과 테이블명 및 컬럼명 규칙:
생성되는 최종 고객 세그먼트 프로파일 테이블은 cymbal.unique_segment_profiles여야 합니다.
이 테이블에는 customer_id, age, gender, loyalty_member, text_sentiment, age_group, gender_segment, loyalty_status, 그리고 이들을 결합한 persona_age_group_profile 컬럼이 포함되어야 합니다.
persona_age_group_profile 컬럼은 age_group, gender_segment, loyalty_status 값을 밑줄로 연결하여 "Older_Adult_FEMALE_LOYAL"과 같은 형태로 생성되어야 합니다.


> **Note**: 이번 단계(2.1 고객 데이터 EDA 및 세분화 로직 정의)는 Option 1. Notebooks 와 Option 2. Data Canvas 두 가지 옵션 중 하나를 선택해 수행하는 단계입니다. 두 옵션 중 하나로 태스크를 완성하면 통과입니다!

**옵션 1: Notebook**

Notebook을 선택해 태스크를 수행하는 경우, 이전 단계에서 사용한 Notebook에서 태스크를 이어갑니다. 
인구통계 정보가 포함된 customers 테이블을 사용합니다. 먼저 테이블 구조를 간단히 살펴보겠습니다.

**2.1.1 고객 세그먼트 프로파일 식별**

노트북에 적힌 지침에 따라 cymbal.multimodal_customer_reviews 테이블과 cymbal.customers 테이블을 customer_id를 기준으로 조인하고, 위에서 정의한 age_group, gender_segment, loyalty_status, text_sentiment 컬럼을 생성합니다.
최종적으로 persona_age_group_profile 컬럼을 포함하는 cymbal.unique_segment_profiles 테이블을 생성합니다.

힌트: Python 클라이언트를 사용하여 BigQuery 쿼리를 실행하거나, 노트북 내에서 %%bigquery 매직 명령어를 활용할 수 있습니다.

Gemini 활용 가이드: Gemini에게 테이블 조인, 새로운 컬럼 생성 (조건부 로직 포함), 그리고 최종적으로 persona_age_group_profile 컬럼을 생성하는 SQL 쿼리를 요청할 수 있습니다.

결과 확인: 생성된 cymbal.unique_segment_profiles 테이블의 스키마와 데이터를 샘플링하여 예상대로 컬럼이 생성되었는지, persona_age_group_profile의 형식이 올바른지 확인합니다.

> **Note**: 노트북에서 새 코드 셀을 추가하신 다음, 이 주제를 시각화하는 Python 코드를 Gemini를 통해 작성해보세요. Gemini에게 위의 예시와 비슷한 형태의 시각화 코드를 생성하도록 요청할 수 있습니다.

셀 사이의 공간에 마우스를 대고, 새로 나타난 "+ Code" 버튼을 누릅니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/instr-task2/qwiklabs/instructions/images/task2_image3.png" alt="task2_image3.png"  width="624.00" />

"generate"을 클릭한 후, 나타난 입력창에 Gemini에게 요청할 프롬프트를 입력합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/instr-task2/qwiklabs/instructions/images/task2_image4.png" alt="task2_image4.png"  width="624.00" />

예시 프롬프트:
"BigQuery에서 cymbal.multimodal_customer_reviews 테이블과 cymbal.customers 테이블을 customer_id로 조인하는 코드를 만들어줘."

프롬프트를 입력한 후, 엔터 키를 눌러 프롬프트를 전송합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/instr-task2/qwiklabs/instructions/images/task2_image5.png" alt="task2_image5.png"  width="624.00" />

요청한 프롬프트에 대해 Gemini가 생성한 코드를 실행할 수 있습니다.

**2.1.2 시각화**

이 단계에서는 자유롭게 데이터 탐색을 하시면서, 앞선 단계에서 생성한 unique_segment_profiles 테이블, customers 테이블을 살펴봅니다. 

이 섹션에 작성된 코드 셀은 각 세그먼트에 속하는 고객의 수를 시각화하는 코드입니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/4a0efc497af584e5.png" alt="4a0efc497af584e5.png"  width="624.00" />

위와 같은 Bar Chart로 고객 세그먼트의 집계를 조회할 수 있습니다.

**2.1.3 추가 시각화**

다음과 같은 주제로 EDA를 자유롭게 수행해봅니다 :

주제 1: 연령대와 충성도 간의 관계 분석
age_group과 loyalty_status의 조합이 어떻게 고객 수에 영향을 미치는지 시각화하여, 특정 연령대의 고객이 더 충성도가 높은 경향이 있는지 등을 분석합니다.

주제 2: 세그먼트별 지리적 분포 시각화
address_city 정보를 활용하여 각 persona_age_group_profile별 고객들이 특정 도시에 집중되어 있는지 또는 넓게 분포되어 있는지 시각화하여 지리적 특성을 파악합니다.


**옵션 2: Data Canvas**

BigQuery Data Canvas는 시각적인 인터페이스를 통해 복잡한 데이터 조인, 변환, 집계 및 시각화를 수행할 수 있는 도구입니다. 이 옵션을 통해 고객 데이터의 EDA 및 세그먼트 생성 작업을 시각적으로 진행합니다.

**2.2.1 Data Canvas에 테이블 추가 및 조인 수행**

화면 왼쪽 탐색 패널에서 Data Canvas를 클릭하거나, + 버튼을 눌러 새 Canvas를 생성합니다.
Data Canvas를 처음 사용하는 경우, API 사용 설정 버튼이 나타납니다. "" 버튼을 클릭하여 Data Canvas API를 활성화합니다.

Data Canvas 화면의 Recents 아래 multimodal_customer_reviews 테이블과 customers 테이블을 각각 캔버스에 추가합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/f50d21a3cf64f30.png" alt="f50d21a3cf64f30.png"  width="621.50" />

    
Recents &gt; customers 테이블을 클릭합니다.   
만약 Recents 아래에 customers 테이블이 조회되지 않는 경우, "Search for data"  버튼 클릭합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/a5f278cbd310a6cb.png" alt="a5f278cbd310a6cb.png"  width="511.50" />

검색창에 "customers"를 입력한 다음 엔터키를 누르고, ‘project_id'.cymbal.customers 우측의 "Add to canvas" 버튼을 클릭합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/ff3d9b3a0d051e05.png" alt="ff3d9b3a0d051e05.png"  width="624.00" />

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/585217364ecd80bc.png" alt="585217364ecd80bc.png"  width="299.50" />

화면 우측 하단의 "+" 버튼을 누르고, "New search"를 클릭한 후 캔버스 내의 빈 공간을 클릭합니다. (클릭한 빈 공간에 테이블을 추가하게 됩니다.)  
같은 방법으로 multimodal_customer_reviews 테이블을 추가합니다.   
customer 테이블의 구조를 간단히 살펴보겠습니다. "Preview"란을 클릭하면 테이블의 데이터를 일부 조회할 수 있습니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/99c8631e4ad02846.png" alt="99c8631e4ad02846.png"  width="624.00" />

      
**2.2.2 고객 세그먼트 프로파일 식별**

customers 테이블 노드를 클릭한 후 나타나는 Join 옵션을 선택합니다. 이어서 On this canvas 아래 multimodal_customer_reviews 테이블 노드를 클릭 후 OK로 확정합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/92fe9ee2bd93718.png" alt="92fe9ee2bd93718.png"  width="500.50" />

두 테이블 사이에 Join 블록이 생성되면, 조인 대상이 되는 두 테이블(예 - t1, t2)에서 ON t1.customer_id = t2.customer_id와 같은 구문이 있는지 확인합니다. 

확인 후 Run 버튼을 클릭합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/eb3142b78394b062.png" alt="eb3142b78394b062.png"  width="519.69" />

조인된 결과 블록을 클릭한 후 Query these results를 선택합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/762ca3c5e22064c3.png" alt="762ca3c5e22064c3.png"  width="527.50" />

새로운 창의 쿼리 편집기에서 직접 SQL을 작성하거나, Gemini에게 자연어 프롬프트를 사용하여 컬럼 생성을 요청합니다.

* sentiment_json_string 컬럼에서 '$.sentiment'를 추출하여 'text_sentiment'라는 새 컬럼을 만듭니다.
* age 컬럼을 기반으로 40세 미만은 'Younger_Adult', 40세 이상은 'Older_Adult'로 구분하는 'age_group' 컬럼을 추가합니다.
* gender 컬럼의 값을 대문자로 변환하여 'gender_segment'라는 컬럼으로 만듭니다.
* loyalty_member가 True면 'LOYAL', False이면 'NON_LOYAL'인 'loyalty_status' 컬럼을 추가합니다.

Gemini 활용 가이드: \
Gemini에게 자연어 프롬프트를 사용하여 위 요구사항을 만족하는 SQL 쿼리 생성을 요청할 수 있습니다. \
예를 들어: "gender 컬럼의 값은 대문자로 'gender_segment'라는 칼럼으로 만들고, loyalty_member가 True면 'LOYAL', False이면 'NON_LOYAL'인 'loyalty_status' 컬럼을 추가해 줘." \
쿼리를 실행하여 결과를 확인하고, 예상대로 새로운 컬럼들이 생성되었는지 검토합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/bbb2804f0f932c0e.png" alt="bbb2804f0f932c0e.png"  width="624.00" />

Run 버튼으로 쿼리를 실행해 결과를 확인합니다.

이제 위에서 생성한 ‘age_group, gender_segment, loyalty_status' 칼럼을 이어 붙여 "Older_Adult_FEMALE_LOYAL"과 같은 형태로 고객 세그먼트를 나타내는 새로운 칼럼을 생성합니다.

다시 해당 노드(마지막으로 작업을 수행한 노드)를 ㄷ마지막으로 작업한 노드를 클릭한 후 하단의 Query these results 버튼을 클릭합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/371f475802604197.png" alt="371f475802604197.png"  width="544.50" />

이제 위에서 생성한 ‘age_group, gender_segment, loyalty_status' 칼럼을 이어 붙여 "Older_Adult_FEMALE_LOYAL"과 같은 형태로 고객 세그먼트를 나타내는 새로운 칼럼을 생성합니다.

Gemini에게 자연어 프롬프트를 사용하여 그룹화 및 프로파일 생성 쿼리를 요청하거나 직접 SQL을 작성합니다.

쿼리를 실행하여 의도한 형식에 맞게 고유한 페르소나 프로파일 칼럼이 생성되었는지 확인합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/c3c46a68c5b74c68.png" alt="c3c46a68c5b74c68.png"  width="624.00" />

**2.2.3 결과 테이블 저장** 

최종 쿼리 결과(Query Results) 블록을 클릭합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/e26160370276cd5d.png" alt="e26160370276cd5d.png"  width="624.00" />

상단 또는 하단에 있는 Save results &gt; BigQuery table을 선택합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/3197356bdb34b06a.png" alt="3197356bdb34b06a.png"  width="624.00" />

Dataset은 cymbal을, Table name은 unique_segment_profiles로 지정합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/1b96c93297ec2f27.png" alt="1b96c93297ec2f27.png"  width="508.50" />

Save 버튼을 눌러 테이블을 저장합니다.

이 작업은 Data Canvas를 통해 정의된 고유 세그먼트 프로파일을 포함하는 cymbal.unique_segment_profiles 테이블을 BigQuery에 생성합니다.

**2.2.4 시각화 블록 추가 (선택 사항)**

최종 쿼리 결과 블록을 클릭하고 Visualize를 선택합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/7db194431f406e7b.png" alt="7db194431f406e7b.png"  width="624.00" />

다양한 차트 유형(예: Bar Chart, Pie Chart)을 사용하여 고객 분포를 시각적으로 탐색합니다. 이를 통해 어떤 세그먼트가 가장 많은 고객을 포함하는지 등을 파악할 수 있습니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/86ffa77b507ffa68.png" alt="86ffa77b507ffa68.png"  width="491.10" />

위의 예시는 Auto-generate 기능을 사용해 자동으로 생성된 차트로 LOYAL, NON_LOYAL 고객의 세그먼트별 분포를 나타냅니다.

목표를 확인하려면 **진행 상황 확인을 클릭**하세요.
<ql-activity-tracking step=8>
Create tables for Customer Personas
</ql-activity-tracking>

**2.3 Gemini로 상세 페르소나 생성**

이전 단계에서 Data Canvas 또는 BigQuery Studio를 통해 cymbal.unique_segment_profiles 테이블을 성공적으로 생성했다면, 이제 이 테이블을 기반으로 Gemini 모델을 호출하여 각 세그먼트의 상세 페르소나를 생성합니다.

노트북에서 "Gemini를 사용한 상세 페르소나 생성" 이라는 헤더 블록 아래의 셀부터 실행하여, 앞에서 정의한 각각의 페르소나에 대해 BigQuery ML의 ML.GENERATE_TEXT 함수와 Gemini 모델을 사용해 다각적인 페르소나 분석을 생성하고, 출력값을 cymbal.segment_level_gemini_analysis 테이블에 저장합니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/instr-task2/qwiklabs/instructions/images/task2_image6.png" alt="task2_image6.png"  width="624.00" />

> **참고**: 이후 단계는 BigQuery ML의 ML.GENERATE_TEXT 함수를 반복적으로 호출하는 로직이 필요하며, 현재 BigQuery Data Canvas 인터페이스 내에서 직접적으로 이 반복 호출을 구성하기 어렵습니다. 따라서 남은 태스크는 BigQuery Studio의 Python 노트북 셀에서 실행하는 것을 권장합니다.

앞에서 정의한 각각의 페르소나에 대해 BigQuery ML의 ML.GENERATE_TEXT 함수와 Gemini 모델을 사용해 다각적인 페르소나 분석을 생성하고, 출력값을 cymbal.segment_level_gemini_analysis 테이블에 저장합니다.



**2.4 최종 고객 인사이트 및 페르소나 정의 테이블 생성**

마지막으로, 주요 인사이트를 담은 테이블과 정리된 페르소나 설명을 담은 테이블을 생성합니다.
해당 부분의 코드는 제공됩니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/bdf6585a0a125882.png" alt="bdf6585a0a125882.png"  width="624.00" />

목표를 확인하려면 **진행 상황 확인을 클릭**하세요.
<ql-activity-tracking step=9>
Create tables for Persona Segment Descriptions
</ql-activity-tracking>

 <img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/337a056e391e2839.png" alt="337a056e391e2839.png"  width="455.95" />



## Task 3: 불만족 고객을 위한 맞춤형 프로모션 상품 추천 및 평가



**개요**

Task 3에서는 Task 1에서 식별된 불만족 리뷰와 Task 2에서 정의된 고객 세그먼트 및 페르소나 정보를 활용하여, 불만족한 고객의 재참여를 유도하기 위해 개인화된 상품 추천을 수행하고, 이 추천을 평가합니다.

**목표**

* 감성 분석 기반 부정적 피드백 고객 식별
* 식별된 고객의 세그먼트 프로파일 및 지리 정보 검색
* 각 세그먼트 및 지역 내 최고 인기 구매 제품을 조회하는 SQL 쿼리 작성
* 맞춤형 프로모션 제품 추천
* Gemini를 사용해 제품 추천을 평가하는 Python 코드 및 SQL 쿼리 작성

<div><ql-warningbox>

**참고**: Task 3은 Task 1, 2에 의존도를 가지고 있어 Task 1, 2를 반드시 완수한 후에 시작해야 합니다.
</ql-warningbox></div>

**노트북 업로드**

1. BigQuery Studio 탐색기 창에서 Notebooks 옆의 점 3개(⋮) 아이콘을 클릭한 후 'URL에서 노트북 업로드(Upload notebook from URL)'를 선택합니다.
<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/instr-task2/qwiklabs/instructions/images/task2_image1.png" alt="task2_image1.png"  width="624.00" />
2. Upload from 에서 URL 선택 후 https://github.com/cheeunlim/dnpursuit_da_hackathon/blob/main/task3.ipynb를 입력합니다.
3. Region: us-central-1을 선택합니다.
4. "Upload" 버튼을 누른 후, 화면 하단의 "Go to notebook" 알림 버튼을 눌러 새로운 노트북 탭을 엽니다. 이 노트북의 셀들을 실행하여 태스크 3을 진행하겠습니다.

**1. Task 3 환경 초기화**

태스크 셋업을 위해 초기 설정 셀을 실행합니다. 이 셀은 필요한 라이브러리를 가져오고, BigQuery 클라이언트를 초기화하며, 이 랩에서 사용될 주요 변수(예: PROJECT_ID, DATASET_ID)를 정의합니다.

**2. 불만족 리뷰 고객 식별**

이 단계에서는 final_customer_insights 테이블의 sentiment_json_string 칼럼에서 감성 분석 결과가 "negative" 혹은 "neutral"인 고객의 customer_id 목록을 추출하고, negative_customers_list라는 BigQuery 테이블로 저장합니다.
제공된 코드를 실행하여 다음 단계로 넘어갑니다.

**3. 불만족 고객의 세그먼트 및 지리적 데이터 검색**

Step 2에서 식별된 불만족 고객에 대한 데이터를 가져옵니다. 

customers 테이블에서 address_city를 추출하고, final_customer_insights에서 persona_age_group_profile 정보를 추출해 negative_customer_segment_data 테이블에 저장합니다.


목표를 확인하려면 **진행 상황 확인을 클릭**하세요.
<ql-activity-tracking step=10>
  Create tables for Negative Customer Segments
</ql-activity-tracking>

**4. 세그먼트, 도시별 인기 제품 추출 및 최종 추천 상품 결정**

각 불만족 고객이 속한 세그먼트(persona_age_group_profile)와 거주 도시(address_city) 내에서 다른 고객들이 가장 많이 구매한 제품을 조회하여 이 제품들을 개인화된 상품 추천에 활용합니다. 각 세그먼트별 상위 2개 인기 제품과 각 도시별 상위 2개 인기 제품을 각각 cymbal.segment_top_products_ranked 테이블과 cymbal.city_top_products_ranked 테이블로 저장하는 SQL 쿼리를 직접 작성합니다.

요구사항:
* cymbal.segment_top_products_ranked 테이블:
컬럼명: persona_age_group_profile, segment_top1_product, segment_top2_product \
final_customer_insights 테이블과 customer_reviews_external 테이블을 조인하여 사용합니다. \
ROW_NUMBER() 윈도우 함수를 사용하여 persona_age_group_profile별로 productId의 구매 횟수(COUNT(product_id))에 따라 순위를 매깁니다. \
상위 1위(rn=1)와 2위(rn=2) 제품의 product_id를 추출하여 각각 segment_top1_product, segment_top2_product 컬럼에 저장합니다. \
* cymbal.city_top_products_ranked 테이블:
컬럼명: address_city, city_top1_product, city_top2_product \
customers 테이블과 customer_reviews_external 테이블을 조인하여 사용합니다. \
ROW_NUMBER() 윈도우 함수를 사용하여 address_city별로 product_id의 구매 횟수(COUNT(product_id))에 따라 순위를 매깁니다. \
상위 1위(rn=1)와 2위(rn=2) 제품의 product_id를 추출하여 각각 city_top1_product, city_top2_product 컬럼에 저장합니다. \

힌트: 두 쿼리 모두 WITH 절을 사용하여 순위가 매겨진 중간 결과를 생성하고, 이후 MAX(CASE WHEN ... END) 패턴을 사용하여 피벗하여 최종 컬럼을 만들 수 있습니다.


목표를 확인하려면 **진행 상황 확인을 클릭**하세요.
<ql-activity-tracking step=11>
Create tables for Top Products
</ql-activity-tracking>

각각 상품 테이블을 합치고, 고객이 리뷰를 남긴 상품과의 중복을 확인합니다. 고객이 리뷰한 상품이 추천 목록에 포함된 경우 이를 제외하고, 포함되지 않은 경우 거주 도시 기반의 두 번째 추천 상품을 제외해 총 3개의 추천 상품 목록으로 정리합니다.

이 단계의 코드는 제공되니 그대로 실행하여 cymbal.final_personalized_recommendations 테이블을 생성하세요.


목표를 확인하려면 **진행 상황 확인을 클릭**하세요.
<ql-activity-tracking step=12>
Create tables for Personalized Recommendations
</ql-activity-tracking>

**5. Gemini로 개인화된 추천 콘텐츠 평가**

이제 고객별로 추천한 상품의 적합도를 평가합니다. 이 평가는 BigQuery에서 Gemini를 활용하며, 고객 세그먼트 페르소나, 추천 상품 이름, 카테고리 정보를 기반으로 합니다.

요구사항:
1. cymbal.temp_recommendation_details 테이블 생성 (제공된 코드 실행): 이 테이블을 먼저 생성하여 필요한 데이터를 준비합니다.
2. 평가 프롬프트 생성 (Python 코드 작성): 각 추천 사항(테이블의 행)마다 Gemini 평가 프롬프트를 동적으로 생성하는 Python 코드를 작성합니다. GEMINI_EVALUATION_PROMPT_TEMPLATE 변수를 활용하여 prompt 컬럼을 만드세요.
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

* 결과: 생성된 프롬프트 DataFrame은 customer_id, recommendation_rank, product_title, prompt 컬럼을 포함해야 합니다.
3. 프롬프트 저장: 생성된 프롬프트 DataFrame을 temp_gemini_evaluation_prompts라는 임시 BigQuery 테이블에 저장합니다.
4. Gemini 모델 호출 및 평가 테이블 생성 (SQL 쿼리 작성): temp_gemini_evaluation_prompts 테이블의 프롬프트를 사용하여 BigQuery ML의 ML.GENERATE_TEXT 함수를 호출하는 SQL 쿼리를 작성합니다.
* JSON 결과 파싱 및 최종 테이블 (cymbal.gemini_recommendation_evaluation) 생성 로직은 제공되니 이를 활용하여 쿼리를 완성하세요.
* 모델 이름: GEMINI_MODEL_NAME 변수를 사용합니다.
* STRUCT 옵션: STRUCT(0.5 AS temperature, 1024 AS max_output_tokens, TRUE AS flatten_json_output)를 사용합니다.

모든 과정을 마치고, Gemini의 평가 내용을 조회합니다. 추후 다른 Task(Task 6)에서 이번 태스크의 내용을 일부 활용할 수 있도록 BigQuery 테이블로 저장되었는지 확인합니다.

목표를 확인하려면 **진행 상황 확인을 클릭**하세요.
<ql-activity-tracking step=13>
Create tables for Recommendation Evaluations
</ql-activity-tracking>


**Checkpoint Logs**

Checkpoint 1.

* Table ‘cymbal.negative_customers_list', ‘cymbal.negative_customer_segment_data' 생성 여부 확인

Checkpoint 2.

* Table ‘cymbal.segment_top_products_ranked', ‘cymbal.city_top_products_ranked' 생성 여부 확인

Checkpoint 3.

* Table ‘cymbal.final_personalized_recommendations' 생성 여부 확인

Checkpoint 4. 

* Table ‘cymbal.gemini_recommendation_evaluation' 생성 여부 확인
* Column ‘gemini_raw_evaluation'의 값 !=None 여부 확인


## **Task4: 추가적인 탐색적 데이터 분석(EDA)**

**개요**

이전 불만족스러운 경험을 한 고객들을 위해 맞춤형 추천 서비스의 개선을 진행하고자 합니다. 더 나은 상품 추천을 수행할 수 있는 모델 생성 및 훈련을 위해 탐색적 데이터 분석을 수행하기로 결정했고, 다음과 같은 방법을 활용하여 진행하기로 했습니다.

1. 빅쿼리 스튜디오 \+ ML Model을 활용한 탐색적 데이터 분석  
2. \[옵션\] 데이터 인사이트와 데이터 캔버스를 활용한 탐색적 데이터 분석

분석 업무를 맡은 여러분들은 불만족한 고객들이 만족할 수 있도록 상품 추천 모델을 만들기 위한 탐색적 데이터 분석을 진행해야 합니다. 아래 과정에서 챌린지를 해결하고 그 과정에서 자유롭게 탐색적 데이터 분석을 진행합니다.

| Note: 이번 단계는 Task 5 상품 추천 모델을 위한 데이터 분석을 목표합니다. 아래 가이드는 어떻게 사용하는지 방법을 알려드리는 예시일 뿐이며 방법을 숙지 후 비즈니스에 어떻게 적용할지 역할에 몰입 하여 자유롭게 데이터 분석을 진행합니다. 또한, 옵션에 제한되지 않고 자유롭게 조합해 데이터 분석을 진행해도 됩니다. |
| :---- |

	

**목표**

* BigQuery Studio와 ML Model을 활용한 데이터 분석을 위해 다양한 모델과 활용방법을 경험합니다.  
* \[옵션\] 데이터 인사이트와 데이터 캔버스를 활용한 데이터 분석을 경험합니다.  
* 분석 결과를 CSV 파일로 Google Cloud Storage 내 Bucket에 저장합니다. 


### **고객리뷰 테이블을 cymbal 데이터세트 내에 생성하기**

다음은 고객리뷰 테이블을 cymbal 데이터세트 내에 생성하는 과정입니다. 이때, 테이블명은 **customer\_review**로 생성합니다.

1. 빅쿼리 콘솔에서 <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task4_img_001.png" alt="task4_img_001.png" /> 다음 버튼을 클릭합니다.  
2. <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task4_img_002.png" alt="task4_img_002.png" />  
   1. Google Cloud Storage에서 your-project-id-bucket/review/customer\_reviews.csv 경로를 지정합니다.  
   (일반적으로 your-project-id-bucket, 예: qwiklabs-gcp-xx-xxxxx-bucket 형식)
   2. Schema는 Auto Detect를 설정합니다.  
3. <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task4_img_003.png" alt="task4_img_003.png" />  
   1. Advanced options에서 Quoted newlines를 체크합니다.  
4. <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task4_img_004.png" alt="task4_img_004.png" />클릭하여 테이블 생성을 완료합니다.  
5. 결과로는 이렇게 구성이 되어야합니다.  
   <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task4_img_005.png" alt="task4_img_005.png" />

### **\[옵션\] 데이터 인사이트와 데이터 캔버스를 활용한 탐색적 데이터 분석**

다음은 데이터 인사이트와 데이터 캔버스를 어떻게 생성하고 다루는지 기능 소개합니다.

1. 데이터 인사이트 다루기  
   1. BigQuery의 왼쪽 패널에서 <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task4_img_028.png" alt="task4_img_028.png" />클릭 후 분석하고자 데이터세트에 진입 한 후 테이블을 클릭합니다.  
   2. 이후 <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task4_img_029.png" alt="task4_img_029.png" />버튼을 클릭합니다.  
   3. <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task4_img_030.png" alt="task4_img_030.png" />클릭해 API 활성화합니다.  
      1. Gemini for Google Cloud API  
      2. BigQuery Unified API   
   4. <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task4_img_031.png" alt="task4_img_031.png" />클릭 후 리전 **us-central1** 설정 후 생성합니다.   
   5. <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task4_img_032.png" alt="task4_img_032.png" />  
   6. 인사이트 결과의 쿼리를 복사해 실행하면 다음처럼 결과가 나옵니다.  
   7. <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task4_img_033.png" alt="task4_img_033.png" />
2. 데이터 캔버스 다루기  
   1. BigQuery Studio(<img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task4_img_034.png" alt="task4_img_034.png" />)에서 <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task4_img_035.png" alt="task4_img_035.png" />클릭합니다.  
   2. <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task4_img_036.png" alt="task4_img_036.png" />을 클릭해 활성화합니다.  
      1. Gemini for Google Cloud API  
      2. BigQuery Unified API   
   3. 분석하고자 하는 테이블을 클릭합니다.  
   4. <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task4_img_037.png" alt="task4_img_037.png" />  
      1. 다음 버튼을 클릭해 자유롭게 진행하는 방향으로 시도합니다.  
   5. <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task4_img_038.png" alt="task4_img_038.png" />  
      1. 특정한 쿼리문 작성할 필요없이 그림그리듯 분석이 가능합니다.  
      2. 원하는 결과를 노트북으로 보내보는 과정까지 해보겠습니다. <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task4_img_039.png" alt="task4_img_039.png" />을 클릭 후 리전 **us-central1**로 저장합니다.  
      3. BigQuery 왼쪽 패널에서 <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task4_img_040.png" alt="task4_img_040.png" />클릭하면 생성한 노트북을 확인할 수 있습니다. 다음은 위 결과를 노트북에 코드로 저장된 예시입니다. 이는, 예시일 뿐이며 여러분들이 자유롭게 진행한 캔버스 결과에 따라 상이합니다.  
         <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task4_img_041.png" alt="task4_img_041.png" />


### **빅쿼리 스튜디오와 ML Model을 활용한 탐색적 데이터 분석**

다음은 ML Model에서 필요한 모델을 생성해 데이터 분석에 활용하는 예제입니다. 예제는 간단한 참고용이므로 사용 방법 숙지를 중심하되 자유롭게 데이터를 분석하고 어떤 모델을 택할지 택하여 분석합니다. 지원 모델은 다음 [링크를](https://cloud.google.com/bigquery/docs/bqml-introduction?hl=ko#generative_ai_and_pretrained_models) 참조하세요.

1. 고객, 고객리뷰, 상품 데이터 분석을 진행합니다.  
   1. 옵션인 데이터 인사이트와 데이터 캔버스도 활용해 분석을 경험해보세요\!  
   2. 임베딩 활용해 품질문제, 배송문제 등 검색을 통해 분석합니다.  
2. 해당 예제에서는 다음 데이터를 추가하기로 결정했습니다.  
   1. 다음과 같은 데이터 보강을 진행합니다.  
      1. 평점과 리뷰를 분석해 부정적인지 긍정 또는 중립적인지 판단한 데이터   
      2. 품질문제, 배송문제 등 군집화된 분류 데이터  
      3. 고객들의 리뷰에서 개선점과 고객이 좋아할 특성을 정의한 데이터  
      4. 제품 테이블에 제품의 장점, 단점 그리고 특성을 정의한 데이터

### **임베딩과 유사도 검색 활용해 분석하기**

<img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task4_img_006.png" alt="task4_img_006.png" />

1. BigQuery Studio(<img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task4_img_007.png" alt="task4_img_007.png" />)에서 <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task4_img_008.png" alt="task4_img_008.png" />버튼을 클릭합니다.

<img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task4_img_009.png" alt="task4_img_009.png" />

2. 데이터세트(Dataset name)는 cymbal로 지정 및 모델명은 embedding\_model 로 작성합니다.  
3. Creation method에서 **Connect to Vertex AI LLM service and CloudAI services** 체크 후 <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task4_img_010.png" alt="task4_img_010.png" /> 클릭합니다.  
4. Model options에서 다음과 같이 설정 후 생성합니다.  
* **Model type** : Google and Partner Models  
* **Remote Connection** : Default connection  
* **REMOTE\_SERVICE\_TYPE** : text-multilingual-embedding-002

| Note: text-multilingual-embedding-002는 다국어 대상으로 임베딩을 생성하는 모델입니다. 텍스트 임베딩 지원 모델에 관한 자세한 내용은 [링크](https://cloud.google.com/vertex-ai/generative-ai/docs/model-reference/text-embeddings-api?hl=ko) 참조하세요. |
| :---- |

  Tip) 모델 생성시 다음과 같은 에러가 출력된다면 다음을 따르세요.

  <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task4_img_011.png" alt="task4_img_011.png" />

1. 위에서 출력된 에러메세지 내에 account 계정을 복사합니다. 해당 예제에서는 bqcx-58118112739-tsw7@gcp-sa-bigquery-condel.iam.gserviceaccount.com 로 파악할 수 있습니다.  
2. IAM 서비스로 이동합니다.  
3. <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task4_img_012.png" alt="task4_img_012.png" />에서 Grant access 클릭합니다.  
4. <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task4_img_013.png" alt="task4_img_013.png" /> 이와 같이 Add principals에서 위 계정을 추가합니다.  
5. <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task4_img_014.png" alt="task4_img_014.png" /> 이와 같이 Vertex AI User 역할을 부여합니다.  
6. <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task4_img_015.png" alt="task4_img_015.png" /> 클릭해 저장하고 다시 돌아와 모델을 재성합니다.  
5. <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task4_img_016.png" alt="task4_img_016.png" /> 다음 버튼을 클릭하여 쿼리창을 엽니다.  
6. 다음 쿼리에서 **PROJECT\_ID, DATASET\_NAME, MODEL\_NAME, TABLE\_NAME** 값을 수정해 임베딩한 테이블을 생성합니다.

```sql
CREATE OR REPLACE TABLE `PROJECT_ID.DATASET_NAME.NEW_TABLE_NAME` AS
SELECT * FROM ML.GENERATE_EMBEDDING(
MODEL `PROJECT_ID.DATASET_NAME.MODEL_NAME`,
(SELECT review_text AS content, customer_id, customer_review_id
FROM `PROJECT_ID.DATASET_NAME.TABLE_NAME`
)
)
```

* **PROJECT\_ID** : 현재 프로젝트 명  
* **DATASET\_NAME** : cymbal  
* **NEW\_TABLE\_NAME** : embeded\_table  
* **MODEL\_NAME** : embedding\_model  
* **TABLE\_NAME** : customer\_review  
7. <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task4_img_017.png" alt="task4_img_017.png" /> Run 버튼을 클릭하여 동작합니다.  
8. 이후 임베딩 결과 테이블을 바탕으로 벡터 검색을 수행합니다. 아래 쿼리는 품질 불량으로 추측되는 항목을 유사도에 따라 보입니다.  동작 후 실제로 해당하는지 검증합니다.

| Note: 만일 컬럼의 양이 많다면 벡터 색인을 사용할 수 있습니다. 자세한 정보는 [링크](https://cloud.google.com/bigquery/docs/vector-index?hl=ko) 참조하세요.  |
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
`PROJECT_ID.DATASET_NAME.EMBEDDED_TABLE` AS t,
ML.GENERATE_EMBEDDING(
MODEL `PROJECT_ID.DATASET_NAME.MODEL_NAME`,
(SELECT '품질 불량' AS content) # 여기에 검색어를 입력합니다.
) AS query_embed
```

* **PROJECT\_ID** : 현재 프로젝트 명  
* **DATASET\_NAME** : cymbal  
* **MODEL\_NAME** : embedding\_model  
* **EMBEDDED\_TABLE** : embeded\_table

#### **CHALLENGE 1 : 품질불량에 따른 유사도를 내림차순 그리고 상위 5개 항목을 결과로 출력하세요.**

결과의 예는 다음처럼 나오게 됩니다.

<img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task4_img_018.png" alt="task4_img_018.png" />

Click Check my progress to verify the objective.

\<ql-activitiy-tracking **step=25**\>

쿼리에 order by similarity\_score desc, limit 5 가 위 쿼리에 있는지

\</ql-activitiy-tracking\>

### **각 리뷰별 카테고리 군집화 데이터 보강하기**

1. BigQuery Studio(<img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task4_img_019.png" alt="task4_img_019.png" />)에서 <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task4_img_020.png" alt="task4_img_020.png" />클릭합니다.  
2. 데이터세트(Dataset name)는 cymbal로 지정 및 모델명은 gemini\_model 로 작성합니다.  
3. Creation method에서 **Connect to Vertex AI LLM service and CloudAI services** 체크 후 <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task4_img_021.png" alt="task4_img_021.png" /> 클릭합니다.  
4. Model options에서 다음과 같이 설정 후 생성합니다.  
* **Model type** : Google and Partner Models  
* **Remote Connection** : Default connection  
* **REMOTE\_SERVICE\_TYPE** : gemini-2.5-flash

| Note: gemini-2.5-flash는 생성형 AI로 다양한 역할을 합니다. 자세한 내용은 [링크](https://cloud.google.com/vertex-ai/generative-ai/docs/models/gemini/2-5-flash?hl=ko) 참조하세요 |
| :---- |

5. 고객들의 리뷰를 참고해 군집화한 카테고리를 생성합니다.  
6. 다음 쿼리에서 **PROJECT\_ID, DATASET\_NAME, MODEL\_NAME, TABLE\_NAME** 아래와 같이 설정후 실행합니다.

```sql
SELECT
customer_review_id,
customer_id,
issue_category
FROM
AI.GENERATE_TABLE(
MODEL `PROJECT_ID.DATASET_NAME.MODEL_NAME`,
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
`PROJECT_ID.DATASET_NAME.TABLE_NAME` AS t
WHERE
t.rating < 3
),
STRUCT(
"issue_category STRING" AS output_schema,
8192 AS max_output_tokens
)
)
```

* **PROJECT\_ID** : 현재 프로젝트 명  
* **DATASET\_NAME** : cymbal  
* **MODEL\_NAME** : gemini\_model  
* **TABLE\_NAME** : customer\_review

실행시 다음과 같은 결과로 출력됩니다.

<img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task4_img_022.png" alt="task4_img_022.png" />

9. 이후 결과를 테이블에 추가 합니다.

```sql
CREATE OR REPLACE TABLE `PROJECT_ID.DATASET_NAME.cgenf_table` AS (
SELECT
customer_review_id,
customer_id,
issue_category
FROM
AI.GENERATE_TABLE(
MODEL `PROJECT_ID.DATASET_NAME.MODEL_NAME`,
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
`PROJECT_ID.DATASET_NAME.TABLE_NAME` AS t
WHERE
t.rating < 3
),
STRUCT(
"issue_category STRING" AS output_schema,
8192 AS max_output_tokens
)
)

)
```

* **PROJECT\_ID** : 현재 프로젝트 명  
* **DATASET\_NAME** : cymbal  
* **MODEL\_NAME** : gemini\_model  
* **TABLE\_NAME** : customer\_review

### **고객들의 리뷰를 분석해 개선점을 정의한 데이터 보강하기**

1. 위 과정에서 사용한 생성형 모델을 사용합니다.  
2. 고객들의 리뷰를 참고해 개선방안과 좋아할만한 특성을 분석한 텍스트를 생성합니다.

```sql
SELECT
generated_output.customer_id,
generated_output.customer_review_id,
generated_output.ml_generate_text_result.candidates[0].content.parts[0].text AS improvement_points
FROM
ML.GENERATE_TEXT(
MODEL `PROJECT_ID.DATASET_NAME.MODEL_NAME`,
(SELECT
customer_id,
customer_review_id,
CONCAT(
'''
다음 리뷰를 참고해서 고객이 불만족한 부분을 개선할 수 있는 구체적인 방안 1개와 고객이 좋아할 법한 상품 특성을 1개 제시해줘 양식은 다음과 같아. 양식을 무조건 지켜서 작성해줘.
개선방안 :
고객이 좋아할 법한 특성 :
참고할 리뷰내용 :
''', review_text) AS prompt
FROM `PROJECT_ID.DATASET_NAME.TABLE_NAME`
WHERE rating < 3),
STRUCT(
0.2 AS temperature,
2048 AS max_output_tokens )
) AS generated_output
```

* **PROJECT\_ID** : 현재 프로젝트 명  
* **DATASET\_NAME** : cymbal  
* **MODEL\_NAME** : gemini\_model  
* **TABLE\_NAME** : customer\_review

해당 결과는 다음과 같이 출력됩니다.

<img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task4_img_023.png" alt="task4_img_023.png" />

#### **CHALLENGE 2 : 위 쿼리 결과를 ‘improv\_table’이라는 이름으로 테이블 생성하세요. 이때, 데이터 보강되는 컬럼명은 ‘improvement\_points’로 지정합니다.** 

Click Check my progress to verify the objective.

\<ql-activitiy-tracking **step=25**\>

Check improv\_table 존재하는지

\</ql-activitiy-tracking\>

3. **cgenf\_table**과 위 챌린지에서 생성한 **improv\_table**를 조인해 보강한 데이터를 하나로 합칩니다. 이때 설정값 **PROJECT\_ID, DATASET\_NAME** 을 바꾼 후 진행합니다.

```sql
SELECT
  t1.*,
  t2.issue_category
FROM `PROJECT_ID.DATASET_NAME.cgenf_table` as t2
JOIN `PROJECT_ID.DATASET_NAME.improv_table` as t1 
ON t1.customer_review_id = t2.customer_review_id
```

* **PROJECT\_ID** : 현재 프로젝트 명  
* **DATASET\_NAME** : cymbal  
4. <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task4_img_024.png" alt="task4_img_024.png" /> 클릭 후 <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task4_img_025.png" alt="task4_img_025.png" />로 저장합니다. 이후, 파일명은 customer\_review\_enf\_result.csv 로 수정합니다.

#### **CHALLENGE 3 : 제품 테이블에 제품의 장점, 단점 그리고 특성을 정의한 데이터 보강을 진행하세요. 결과는 product\_enf\_result.csv로 저장한 후 qwiklabs-gcp-01-416cbdc92d8c-bucket 버킷에서 task4\_result 폴더 생성하여 그 안에 저장합니다. 그 후, 위에서 생성한 customer\_review\_enf\_result.csv 도 함께 저장합니다.** 

**Hint 1\.** 위에서 배운 **AI.GENERATE\_TABLE** 또는 **ML.GENERATE\_TEXT** 활용하는 방법이 있습니다.

**Hint 2\.** 결과 저장하는 방법은 다음을 참고할 수 있습니다.

1. <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task4_img_026.png" alt="task4_img_026.png" />을 클릭합니다.  
2. <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task4_img_027.png" alt="task4_img_027.png" /> 다음 버튼을 클릭하여 저장 후 파일명을 조건에 맞춰 수정합니다.

Click Check my progress to verify the objective.

\<ql-activitiy-tracking **step=25**\>

Check product\_enf\_result.csv과 customer\_review\_enf\_result이 해당 버킷 경로에 존재하는지

\</ql-activitiy-tracking\>

 


Checkpoint Logs

Checkpoint 1\.

Challenge 1을 위한 쿼리에 order by similarity\_score desc, limit 5 가 위 쿼리에 있는지

Checkpoint 2\.

Challenge 2를 위한 것으로 improv\_table 테이블 생성 및 테이블 내 improvement\_points 컬럼 생성 여부 확인

Checkpoint 3\.

Challenge 3을 위한 것으로 생성되는 프로젝트의 버킷에서 task4\_result 폴더 생성 확인과 product\_enf\_result.csv와 customer\_review\_enf\_result 가 그 폴더 내에 존재하는지 확인


## **Task5: 상품 추천 모델**

**개요**

탐색적 데이터 분석을 통한 결과를 기반하여 데이터 사이언티스트 에이전트를 활용해 상품 추천 모델을 생성하고 활용합니다. 다양한 프롬프트 시도와 데이터를 활용해 여러분의 모델을 만들어보세요.

**목표**

* 데이터 사이언티스트 에이전트 활용해 모델 생성 및 성능 평가  
* 모델을 활용한 불만족 고객 대상 추천 상품 생성(Task 6 연계)  
* Google Cloud Storage에 노트북 파일 저장

### **노트북 생성하기**

먼저, 이 작업을 위한 노트북을 BigQuery Studio에 생성하겠습니다.
<img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task5_img_001.png" alt="task5_img_001.png" />
1. Google Cloud 콘솔에서 **BigQuery Studio**로 이동합니다.  
2. 노트북이 생성된 후 런타임 연결을 진행 후 사전 준비에 생성된 네트워크로 연결이 되면 데이터 사이언스 에이전트를 사용할 준비가 된 것입니다.

### **TASK 4에서 생성한 파일 추가하기**

1. 노트북 콘솔 왼쪽 하단에서 <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task5_img_002.png" alt="task5_img_002.png" /> 클릭합니다.  
2. <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task5_img_003.png" alt="task5_img_003.png" /> 이렇게 저장된 버킷에서 파일을 복사합니다.  
   * gsutil cp \- r gs://YOUR\_BUCKET/task4\_result/ .  
3. <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task5_img_004.png" alt="task5_img_004.png" />이와 같이 추가된 것을 확인할 수 있습니다.


### **데이터 사이언스 에이전트 활용하기**

4. 노트북 우측 상단에 보이는 <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task5_img_005.png" alt="task5_img_005.png" />에서 <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task5_img_006.png" alt="task5_img_006.png" />**Gemini** 버튼을 클릭하여 채팅 대화상자를 엽니다.  
   **참고:** 프롬프트 입력하는 우측에서 <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task5_img_007.png" alt="task5_img_007.png" />**패널로 이동** 버튼을 클릭하여 채팅 대화상자를 노트북 외부의 별도 패널로 이동할 수 있습니다.  
     
5. <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task5_img_008.png" alt="task5_img_008.png" /> 파일 위에 커서를 위치하면 다음과 같이 나타나는 버튼을 클릭해 모델에 필요한 파일을 추가합니다.

   \[TIP\] TASK 4에서 생성한 CSV 파일을 직접 업로드하려면 다음 단계를 따르세요.   
   * <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task5_img_009.png" alt="task5_img_009.png" />왼쪽 **파일** 창에서 <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task5_img_010.png" alt="task5_img_010.png" />**파일 추가**를 클릭합니다.  
   * 필요한 경우 Google 계정을 승인합니다.  
     Colab Enterprise가 파일 탐색을 사용 설정할 때까지 잠시 기다립니다.  
   * 필요한 **파일**을 업로드합니다.  
   * 업로드한 파일에 커서를 올리면 나타나는 <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task5_img_011.png" alt="task5_img_011.png" />**Gemini** 버튼을 클릭합니다.  
   * 위 과정을 따라 필요한 파일을 반복해 업로드합니다.

6. **Gemini** 채팅 대화상자에서 프롬프트를 입력하고 <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task5_img_012.png" alt="task5_img_012.png" /> **보내기**를 클릭합니다. 프롬프트에 대한 아이디어를 얻으려면 [데이터 과학 에이전트 기능](https://cloud.google.com/colab/docs/use-data-science-agent?hl=ko#capabilities)을 검토하고 [샘플 프롬프트](https://cloud.google.com/colab/docs/use-data-science-agent?hl=ko#sample-prompts)를 참고하세요.

| Note: 예를 들어 '업로드한 데이터 분석하고 Matrix Factorization 모델을 생성한 후 모델 평가해줘 그리고 불만족한 고객이 만족할만한 상품을 추천해서 테이블 형태로 만들어줘'라고 입력할 수 있습니다. 해당 모델 설명은 다음 [링크](https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-create-matrix-factorization)를 참조하세요. |
| :---- |

   <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task5_img_013.png" alt="task5_img_013.png" />

7. Gemini가 프롬프트에 대답합니다. 대답에는 실행할 코드 스니펫, 프로젝트에 관한 일반적인 조언, 목표 달성을 위한 다음 단계, 데이터 또는 코드의 특정 문제에 관한 정보가 포함될 수 있습니다.  
   대답을 평가한 후 다음 작업을 할 수 있습니다.  
   * Gemini가 대답에서 코드를 제공하는 경우 다음을 클릭할 수 있습니다.  
     * **수락**을 클릭하여 노트북에 코드를 추가합니다.  
     * **수락 및 실행**을 클릭하여 노트북에 코드를 추가하고 코드를 실행합니다.  
     * **취소**를 선택하여 추천 코드를 삭제합니다.  
   * 필요에 따라 후속 질문을 하고 작업을 계속합니다.  
   * 만일, 작업에 문제가 발생한다면 다음 링크를 참고하여 파일을 다운로드 받은 후 업로드하여 테스트합니다.

####  **노트북 파일 GCS에 저장하기**

8. <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task5_img_014.png" alt="task5_img_014.png" />을 클릭한 후 <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task5_img_015.png" alt="task5_img_015.png" />을 활성화합니다.  
9. 파일 \> 다운로드 \> .ipynb 다운로드 클릭하여 다운로드 합니다.  
10. Google Cloud 콘솔에서 **탐색 메뉴**(☰)로 이동하여 **Cloud Storage \> 버킷**을 선택합니다.  
11. 실습 환경에 제공된 버킷 이름을 클릭한 후 task5 폴더로 진입합니다 (일반적으로 **your-project-id-bucket**, 예: **qwiklabs-gcp-xx-xxxxx-bucket** 형식).  
12. 다음 버킷에서 <img src="https://github.com/mjkong0615/kr-bq-hackathon/raw/main/qwiklabs/instructions/images/task5_img_016.png" alt="task5_img_016.png" /> Upload 버튼을 클릭하여 다운받았던 노트북 파일을 업로드합니다. 이때 업로드할 파일명은 **task5\_result.ipynb** 로 지정합니다.

**CHALLENGE 1 : 해당 챌린지는 Task 6에 사용하기 위한 챌린지입니다. 위에서 생성한 테이블을 테이블 명 ‘product\_recommendations’로 생성하세요. 이때, 고객 아이디는 customer\_id로 추천된 상품은 recommended\_products이라는 컬럼명을 가질 수 있도록 해보세요.**

Click Check my progress to verify the objective.

\<ql-activitiy-tracking **step=25**\>

Upload a file ‘task5\_result’ to Bucket 

\</ql-activitiy-tracking\>

Checkpoint Logs

Checkpoint 1\.

생성된 프로젝트 내 버킷에서 Task5\_result 폴더 내 task5\_result.ipynb가 존재하는지 확인


## Task 6: 다중 소스 데이터를 결합한 지능형 고객 리인게이지먼트

#### Overview

이 작업에서는 이전 단계들에서 생성된 여러 데이터 소스를 실시간으로 결합하여, 이탈 징후를 보이는 '부정적 고객(negative customer)'에게 맞춤화된 추천 이메일을 자동으로 발송하는 파이프라인을 완성합니다.

BigQuery 연속 쿼리(CQ)가 **두 가지 핵심 데이터**를 실시간으로 결합하고, Gemini가 이 모든 문맥을 이해하여 최종 이메일을 생성한 뒤, Application Integration을 통해 실제 발송까지 완료하는 과정을 구축합니다.

<div><ql-infobox>

#### 활용할 데이터 소스

1. **Task 3의 결과 (고객 세그먼트):** 고객의 활동을 분석하여 '이탈 위험' 또는 '부정적 경험' 세그먼트로 분류된 고객 리스트 . 이 테이블에는 customer_id와 segment_name와 고객 세그먼트에서 가장 인기 있거나 선호되는 제품 데이터가 포함됩니다
2. **Task 5의 결과 (맞춤 추천 제품 리스트):** BigQuery ML의 추천 모델이 각 개별 고객을 위해 생성한 최신 맞춤 추천 제품 리스트. 이 테이블에는 customer_id 와 recommended_products가 포함되어 있습니다.
</ql-infobox></div>

#### Objective

이 실습에서는 다음 방법을 배웁니다:

* BigQuery ML 원격 모델(Gemini 2.5 Flash Lite) 생성 및 구성하기
* 사용자 지정 서비스 계정에 BigQuery 및 Pub/Sub 리소스 접근 권한 부여하기
* Application Integration 트리거 생성 및 구성하기
* Gemini를 사용하여 이메일 텍스트를 생성하는 연속 쿼리를 BigQuery에서 생성하기
* 연속 쿼리를 테스트하기 위해 결합된 데이터, negative_customer_recommended_products 에 데이터 추가

Qwiklab의 student 계정에는 아웃바운드 이메일 전송이 제한됩니다. 예상되는 이메일 시나리오를 스크린샷으로 제공합니다. 

#### Setup

이 실습에서는 시뮬레이션이나 데모 환경이 아닌 실제 클라우드 환경에서 직접 실습 활동을 수행할 수 있습니다. 실습 시간 동안 Google Cloud에 로그인하고 액세스하는 데 사용할 수 있는 새로운 임시 사용자 인증 정보가 제공됩니다.
Qwiklab의 student 계정에는 아웃바운드 이메일 전송이 제한됩니다. 

#### 1. BigQuery ML 원격 모델 생성 및 구성

이 작업을 위해 continuous_queries라는 BigQuery 데이터세트와 negative_cutomer_recommended_products라는 새로운 값을 인서트할 빈 테이블을 포함한 여러 리소스가 미리 생성되어 있습니다.

이 작업에서는 워크플로우를 위한 개인화된 이메일 콘텐츠를 생성하기 위해 엔드포인트로 Gemini 2.5 Flash Lite를 사용하는 BigQuery ML 원격 모델을 포함한 추가 BigQuery 리소스를 생성하고 구성합니다.

##### 1.1 BigQuery 원격 Connection 생성

1. Google Cloud 콘솔에서 **Navigation menu**() &gt; **BigQuery**를 클릭합니다.
2. **Explorer** 창에서 **+ Add Data**를 클릭한 다음, **Vertex AI**를 검색합니다. 결과에서 **Vertex AI**를 클릭하고 뜨는 **Bigquery Federation**을 클릭합니다.
3. **Connection type**에서 Vertex AI remote models, remote functions and BigLake (Cloud Resource)를 선택합니다.
4. **Connection ID**에 **continuous-queries-connection**을 입력합니다.
5. **Location type**에서 **Region** &gt; **us-central1**을 선택합니다.
6. **Create connection**을 클릭한 다음, **Go to connection**을 클릭합니다 (페이지 하단 메시지)

![alt text](images/task6_gotoconnection.png)

7. **Connection info** 페이지에서 다음 섹션에서 사용할 **Service account ID**를 복사합니다. 예: bqcx-1054723899402-whbp@gcp-sa-bigquery-condel.iam.gserviceaccount.com


##### 1.2 BigQuery 서비스 계정에 Vertex AI용 IAM 역할 부여

1. Google Cloud 콘솔의 **Navigation menu**()에서 **IAM & Admin** &gt; **IAM**을 선택합니다.
2. **Grant access**를 클릭합니다.
3. **New principals**에 이전 섹션에서 복사한 서비스 계정 ID를 입력합니다 (예: bqcx-1054723899402-whbp@gcp-sa-bigquery-condel.iam.gserviceaccount.com).
4. **Select a role**에서 **Vertex AI** &gt; **Vertex AI User**를 선택합니다.
5. **Save**를 클릭합니다.

##### 1.3 BigQuery ML 원격 모델 생성

1. Google Cloud 콘솔에서 **Navigation menu**() &gt; **BigQuery**를 클릭합니다.
2. **Untitled query**를 클릭하여 빈 쿼리 창에 액세스합니다.
3. BigQuery ML 모델을 생성하기 위해 다음 쿼리를 복사하여 붙여넣고, **Run**을 클릭합니다.
SQL

```sql
CREATE MODEL `Project ID.continuous_queries.gemini_2_5_flash_lite`
REMOTE WITH CONNECTION `Region.continuous-queries-connection`
OPTIONS(endpoint = 'gemini-2.0-flash');
```

**참고:** 서비스 계정 권한(이전 섹션에서 할당함)과 관련된 오류가 발생하면 몇 분 정도 기다린 후 쿼리를 다시 실행하세요.

목표를 확인하려면 **진행 상황 확인을 클릭**하세요.
<ql-activity-tracking step=16>
Create Gemin_2_0_flash Model
</ql-activity-tracking>

#### 2 사용자 지정 서비스 계정에 BigQuery 및 Pub/Sub 리소스 접근 권한 부여

이 작업을 위해 recapture_customer라는 Pub/Sub 토픽과 bq-continuous-query-sa@Project ID.iam.gserviceaccount.com이라는 사용자 지정 서비스 계정을 포함한 여러 리소스가 미리 생성되어 있습니다.

이 작업에서는 이후 작업에서 개인화된 이메일을 생성하고 보내는 데 사용될 BigQuery 데이터세트, 원격 모델 및 Pub/Sub 토픽에 대한 접근 권한을 사용자 지정 서비스 계정에 부여합니다.

##### 2.1 사용자 지정 서비스 계정에 원격 모델 접근 권한 부여


1. Google Cloud 콘솔에서 **Navigation menu**() &gt; **BigQuery**를 클릭합니다.
2. **Explorer** 창에서 **Project ID** 옆의 화살표를 확장합니다.
3. **External connections**를 확장하고, **Region.continuous-queries-connection**을 클릭합니다.
4. **Connection info** 페이지에서 **Share**를 클릭합니다.
5. **Add principal**을 클릭합니다.
6. **New principals**에 사용자 지정 서비스 계정 ID를 입력합니다: bq-continuous-query-sa@Project ID.iam.gserviceaccount.com
7. **Select a role**에서 **BigQuery** &gt; **BigQuery Connection User**를 선택합니다.
8. **Save**를 클릭한 다음, **Close**를 클릭합니다

##### 2.2 사용자 지정 서비스 계정에 BigQuery 데이터세트 접근 권한 부여

1. **Explorer** 창에서 고객 리뷰 테이블을 포함하는 데이터세트의 이름인 continuous_queries를 클릭합니다.
2. **Dataset info** 페이지에서 **Sharing**을 클릭하고 **Permissions**를 선택합니다.
3. **Add principal**을 클릭합니다.

![alt text](images/task6_view_permission.png)

4. **New principals**에 사용자 지정 서비스 계정 ID를 입력합니다: bq-continuous-query-sa@Project ID.iam.gserviceaccount.com
5. **Select a role**에서 **BigQuery** &gt; **BigQuery Data Editor**를 선택합니다.
6. **Save**를 클릭한 다음, **Close**를 클릭합니다.

##### 2.3 사용자 지정 서비스 계정에 Pub/Sub Viewer 및 Pub/Sub Publisher 역할 부여

1. Google Cloud 콘솔에서 **Navigation menu**() &gt; **Pub/Sub**을 검색하고 클릭합니다.
2. recapture_customer 행에서 **More Actions**(세로 점 3개)를 클릭하고, **View permissions**를 선택합니다.
3. **Add principal**을 클릭합니다.
4. **New principals**에 사용자 지정 서비스 계정 ID를 입력합니다: bq-continuous-query-sa@Project ID.iam.gserviceaccount.com
5. **Select a role**에서 **Pub/Sub** &gt; **Pub/Sub Viewer**를 선택합니다.
6. **Add another role**을 클릭합니다.
7. **Select a role**에서 **Pub/Sub** &gt; **Pub/Sub Publisher**를 선택합니다.
8. **Save**를 클릭합니다.

목표를 확인하려면 **진행 상황 확인을 클릭**하세요.
<ql-activity-tracking step=17>
Check Pub/Sub role
</ql-activity-tracking>

#### 3. Application Integration 트리거 생성 및 구성

Application Integration은 Google Cloud의 iPaaS(Integration-Platform-as-a-Service) 솔루션으로, 특정 비즈니스 운영을 지원하기 위해 통합되어야 하는 여러 애플리케이션과 데이터를 연결하고 관리하는 도구 세트를 제공합니다.

트리거(trigger)는 Application Integration에서 작업 또는 작업 시퀀스를 시작하는 외부 이벤트입니다. 이 단계에서는 Pub/Sub 토픽의 이벤트를 기반으로 하는 Pub/Sub 트리거를 사용합니다. 트리거는 통합의 진입점이라고 생각할 수 있으며, 트리거에 연결된 이벤트는 트리거와 관련된 작업이 실행되도록 합니다.

이 작업에서는 Pub/Sub 토픽으로 새 메시지가 전송될 때 통합을 실행하는 Application Integration 트리거를 생성하고 구성합니다. 출력은 negative sentiment 고객에게 전송되는 이메일이 됩니다.


##### 3.1 Pub/Sub 트리거 생성

1. Google Cloud 콘솔 검색창(페이지 상단)에 **Application Integration**을 입력한 다음, 결과 목록에서 **Application Integration**을 클릭합니다.
2. **Get started with Application Integration** 페이지의 **Region**에서 **Region**을 선택합니다.
3. **Quick setup**을 클릭하여 필요한 API를 활성화합니다.
4. **Create integration**을 클릭하고, 통합에 다음 이름을 지정합니다: recommend-customer-products-integration
5. **CREATE**를 클릭합니다.
6. recommend-customer-products-integration 페이지에서 **Triggers**(페이지 상단)를 클릭합니다.
7. **Cloud Pub/Sub**을 선택하고 캔버스를 클릭하여 Pub/Sub 트리거를 추가합니다.
8. 트리거 세부 정보 창의 **Trigger Input &gt; Pub/Sub topic**에 미리 생성된 Pub/Sub 토픽 경로를 추가합니다: projects/Project ID/topics/recapture_customer
9. **Service account**에서 사용자 지정 서비스 계정 ID를 선택합니다: bq-continuous-query-sa@Project ID.iam.gserviceaccount.com

* 목록에 보이지 않으면 **Refresh list**를 클릭하세요.
* **Grant the necessary roles**라는 경고가 표시되면 **Grant**를 클릭하세요.

##### 3.2 Pub/Sub 트리거를 위한 데이터 매핑 변수 구성

1. 캔버스 상단에서 **Tasks**(Triggers 옆)를 클릭합니다.
2. 검색창에 Data Mapping을 입력합니다.
3. 결과에서 **Data Mapping**을 선택하고 캔버스를 클릭하여 **Cloud Pub/Sub Trigger** 아래에 데이터 매핑 작업을 추가합니다.
4. **Cloud Pub/Sub Trigger**의 하단 연결점을 클릭하고 커서를 드래그하여 **Data Mapping**의 상단 연결점에 연결합니다.

* 이제 **Cloud Pub/Sub Trigger** 하단에서 **Data Mapping** 상단으로 흐르는 화살표가 있어야 합니다.

![alt text](images/task6_applicationintegration1.png)

5. 캔버스에서 **Data Mapping** 항목을 클릭하고, **Open Data Mapping Editor**를 클릭합니다.
6. 다음 단계에서는 각각 CloudPubSubMessage.data 유형의 입력 변수 네 개를 만듭니다.

- **[변수 1] message_output**

1. **Input** 아래에서 **Variable or Value**를 클릭합니다.
2. **Variable**을 선택한 다음, **CloudPubSubMessage.data**를 선택합니다. **Save**를 클릭합니다.
3. **Output** 아래에서 **Create a new one**을 클릭합니다.
4. **Name**에 message_output을 입력합니다.
5. **Variable type**에서 **Output from integration**을 선택합니다.
6. **Data type**에서 **String**을 선택합니다.
7. **Blank default value means**에서 **Empty string**을 활성화합니다.
8. **Create**를 클릭합니다.

![alt text](images/task6_applicationintegration2.png)

- **[변수 2] customer_message** 
방금 Input에 함수가 포함되지 않은 변수 하나를 만들었습니다. 이제 Input에 두 개의 함수가 포함된 다른 변수를 만듭니다.

1. **Input** 아래에서 **Variable or Value**를 클릭합니다.
2. **Variable**을 선택한 다음, **CloudPubSubMessage.data**를 선택합니다. **Save**를 클릭합니다.
3. 두 번째 변수 옆의 **Add a function**(+ 아이콘)을 클릭하고, **TO_JSON() -&gt; JSON**을 선택합니다.
4. 두 번째 변수에 대해 **Add a function**(+ 아이콘)을 다시 클릭하고, **GET_PROPERTY(String) -&gt; JSON**을 선택합니다.
5. **.GET_PROPERTY** 옆에서 **Variable or Value**를 클릭합니다.
6. **Value**를 선택하고 customer_message를 입력합니다.

![alt text](images/task6_value.png)

7. 이 변수와 동일한 행에서, **Output** 아래에서 **Create a new one**을 클릭합니다.
8. **Name**에 customer_message를 입력합니다.
9. **Variable type**에서 **Output from integration**을 선택합니다.
10. **Data type**에서 **String**을 선택합니다. **참고:** JSON 함수가 추가되었기 때문에 기본값은 JSON이므로, 데이터 유형을 지침대로 **String**으로 변경해야 합니다.
11. **Blank default value means**에서 **Empty string**을 활성화합니다.
12. **Create**를 클릭합니다.

![alt text](images/task6_variable.png)

- **[변수 3 및 4] customer_email 및 customer_name** 
이전 섹션의 1~12단계를 반복하여 다음 정보를 사용하여 두 개의 변수를 더 만듭니다:



| GET_PROPERTY()의 값 | 출력 이름 |
| --- | --- |
| customer_email | customer_email |
| customer_name | customer_name |

<div><ql-infobox>

**참고:** JSON 함수가 추가되었기 때문에 **Output**의 기본 데이터 유형은 JSON이므로, 이 두 변수 모두에 대해 **Output**의 데이터 유형을 **String**으로 변경해야 합니다.
</ql-infobox></div>

이제 이 Application Integration 트리거에 대해 message_output, customer_message, customer_email, customer_name 네 개의 데이터 매핑 변수가 구성되었습니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/22b44f7f0dfa1df3.png" alt="22b44f7f0dfa1df3.png"  width="624.00" />


#### 3.3 이메일 보내기 작업 추가

1. 화면 상단의 **Data Mapping Task Editor** 옆에 있는 뒤로 가기 화살표(&lt;-)를 클릭하여 캔버스로 돌아갑니다.
2. 브라우저 탭을 복제합니다 (현재 탭에서 마우스 오른쪽 버튼을 클릭하고 **Duplicate(복제)** 선택).
3. Google Cloud Console에서 페이지 상단의 검색창에 **Integration Connectors**를 입력한 다음, 결과 목록에서  Connections(연결) 를 클릭합니다.
4.  Create New(새로 만들기) 를 클릭하여 새 연결을 생성합니다.
5.  Region 으로  {Region} 을 선택하고  Next(다음) 를 클릭합니다. 
6. Connector 드롭다운에서 **Gmail**을 선택합니다.
7.  Connection Name(연결 이름) 에 **send-email**을 입력한 다음,  Next(다음) 를 클릭합니다.
8.  Authentication(인증) 에서 scopes(범위)로 [**https://mail.google.com/**](https://mail.google.com/) 을 선택한 다음,  Next(다음) 를 클릭합니다.
9. 세부 정보를 검토한 후 **Create(만들기)** 버튼을 클릭합니다.

커넥터가 처음 프로비저닝되는 경우 연결 생성에 5~10분이 소요될 수 있습니다.

10. 연결을 생성한 후, **Authorization Required(승인 필요)** 상태를 클릭한 다음  Authorize(승인) 를 클릭하고 학생 ID(Student ID)를 사용하여 로그인합니다.
11.  Continue(계속) 를 클릭한 다음 페이지를 새로고침하여 상태가 녹색 체크 표시와 함께  Active(활성) 로 변경되는 것을 확인합니다.

브라우저의 Application Integration 탭으로 돌아갑니다.

12. 캔버스 상단에서 **Triggers(트리거)** 옆의  Tasks(태스크) 를 클릭합니다.
13. 검색창에 **Gmail**을 입력합니다.
14. 결과에서 **Gmail**을 선택하고 캔버스를 클릭하여 **Data Mappings(데이터 매핑)** 아래에 Gmail 태스크를 추가합니다.
15. **Data Mapping**의 하단 연결점을 클릭하고 커서를 드래그하여 **Gmail**의 상단 연결점에 연결합니다.

이제 **Cloud Pub/Sub Trigger**와 **Data Mapping**을 연결하는 첫 번째 화살표 외에, **Data Mapping** 하단에서 **Gmail** 상단으로 흐르는 두 번째 화살표가 생겼습니다.

<img src="https://raw.githubusercontent.com/mjkong0615/kr-bq-hackathon/refs/heads/main/qwiklabs/instructions/images/6848190bb9b4107c.png" alt="6848190bb9b4107c.png"  width="541.50" />


16. 캔버스에서 **Gmail** 항목을 클릭하여 세부 정보를 확인합니다.
17.  Configure Connector(커넥터 구성) 를 클릭하고, Region(리전)으로  [리전 이름]을 선택한 다음 connection(연결) 드롭다운에서 **send-email**을 선택하고 Next(다음) 를 클릭합니다.
18. Set entities/actions(항목/작업 설정)에서 **gmail.users.drafts.send**를 선택한 다음 Done(완료)을 클릭합니다.
19. recommend-customer-products-integration  페이지의 오른쪽 상단에서 Publish(게시)를 클릭합니다.

![alt text](images/task6_justpublish.png)

위와 같은 description generation 창이 뜨는 경우, `NO, JUST PUBLISH`를 클릭합니다.

목표를 확인하려면 **진행 상황 확인을 클릭**하세요.
<ql-activity-tracking step=18>
Create and publish Application Integration
</ql-activity-tracking>

#### 4. BigQuery에서 Gemini로 이메일 텍스트를 생성하는 연속 쿼리(continuous query) 만들기

이전 태스크에서, BigQuery ML 원격 모델 및 Pub/Sub용 Application Integration 트리거와 같이 통합에 필요한 다양한 구성 요소를 생성하고 구성했습니다. 이 태스크에서는 워크플로우의 마지막 조각을 생성합니다. 즉, 새로 추천되는 제품이 있는지 BigQuery 테이블을 모니터링하고, 해당 고객을 위한 맞춤형 프로모션 이메일을 생성하도록 Gemini에 요청을 보낸 다음, 개인화된 이메일 콘텐츠를 Pub/Sub 주제(topic)에 작성하는 연속 쿼리(continuous query)를 생성합니다.

##### 4.1 BigQuery Enterprise reservation 만들기

![alt text](images/task6_capacitymanagement.png)

* Google Cloud 콘솔에서 **Navigation 메뉴**() &gt; **BigQuery** &gt; **Capacity Management**를 클릭합니다.
* **Create reservation**을 클릭합니다.
* 예약 이름(reservation name)에 다음을 입력합니다: bq-continuous-queries-reservation
* 위치(Location)에서 **Region**을 선택합니다.
* 버전(Edition)에서 **Enterprise**를 선택합니다.
* Max reservation size selector에서 Extra Small (50 slots)을 선택합니다.
* Baseline slots에 **50**을 입력합니다.
* **저장**을 클릭합니다.

##### 4.2 Assignment 만들기

예약이 생성된 후, slot reservation table에서 bq-continuous-queries-reservation 이름의 예약 행을 찾습니다.

* **Actions** (세로 점 3개) 아래에서 **Reservation actions**을 클릭하고 **Create assignment**를 선택합니다.
* **Select an Organization, folder or project**에서 를 클릭하고 이 프로젝트(**Project ID**)를 선택합니다.
* Job type으로 Continuous을 선택합니다.
* **Create**를 클릭합니다.
* bq-continuous-queries-reservation 예약 옆의 화살표를 확장하여 projects/Project ID로 표시되는 새 할당을 볼 수 있습니다.

##### 4.3 BigQuery에서 continuous query 만들기

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
               CURRENT_TIMESTAMP() - INTERVAL 10 MINUTE) as ncs
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

내 진행 상황 확인(Check my progress)을 클릭하여 목표를 확인합니다 

#### 5. 연속 쿼리를 테스트하기 위해 Task3와 데이터와 Task6의 데이터를 가공하여 'negative_customer_recommended_products' 테이블에 데이터 추가하기

마지막 태스크에서는 negative_customer_recommended_products 테이블에 일부 데이터를 추가하여, 고객에게 개인화된 이메일을 보내는 Application integration 작업을 시작함으로써 연속 쿼리를 테스트합니다.

1. BigQuery에서 **제목 없는 쿼리** 오른쪽에 있는 **+** 아이콘(**SQL 쿼리**)을 클릭하여 새 쿼리 창을 엽니다.
2. 다음 쿼리를 복사하여 Task3과 Task6에서 얻은 추천 정보를 JOIN하여 테이블에 데이터를 삽입하고 **실행**을 클릭합니다.

```sql
-- 'negative_customer_recommended_products' 테이블에 테스트 데이터를 삽입합니다.
INSERT INTO `continuous_queries.negative_customer_segment_products` 
(
      customer_id,
      customer_name, 
      customer_email, 
      segment, 
      top_products, 
      recommended_products
) 
SELECT 
      pr.customer_id,
      fpr.customer_name, 
      fpr.customer_email, 
      fpr.segment, 
      fpr.product1 AS top_products,
      pr.recommended_products 
FROM 
`continuous_queries.product_recommendations` AS pr 
JOIN 
`continuous_queries.final_personalized_recommendations` AS fpr
ON pr.customer_name = ncs.customer_name
LIMIT 3; 
```

결과창에 **이 문(statement)이 negative_customer_recommended_products에 1개의 행을 추가했습니다**라는 메시지가 표시되면 이 태스크를 완료한 것입니다.

* 실제 이메일 전송 기능을 활용하기 위해서는 Google Cloud Platform의 일반 사용자 계정이 필요합니다.


![alt text](images/task6_email.png)


워크플로우 구축을 완료 하면, 위와 같이 이메일이 제작됩니다.
이메일로 전송될 추천 제품 테이블에 새 행을 삽입함으로써, negative sentiment을 가진 사용자에 대한 맞춤형 이메일을 보내는 워크플로우를 구축했습니다. 
