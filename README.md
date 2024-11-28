<div style="text-align: center;">

![Static Badge](https://img.shields.io/badge/review-pending-black)
![Static Badge](https://img.shields.io/badge/build-passing-lightgreen)
![Static Badge](https://img.shields.io/badge/charts-passing-lightgreen)

</div>

## Dollar Dalla
**원자재 가격(금, 은, 석유 등), 환율, ETF, 세계 증시 지수, 암호화폐 관련 데이터를 추출하고 분석하여 시각화 하는 프로젝트입니다.** </br><span style="color:grey">This pipeline aims to extract, analyze, and visualize data related to commodity prices (e.g., gold, silver, oil), exchange rates, ETFs, global stock market indices, and cryptocurrencies.</span>

<h3 style="color: black; display: block; background-color: lightgrey;">

Features

</h3>
<strong style="color:salmon">🎄 Dags</strong></br>
ETF, 국제증시지수, 원자재, USD환율, KRW환율, 암호화폐 수집에 대한 각각의 Airflow Dag 파이썬 스크립트</br>
<strong style="color:salmon">🎄 Tables</strong></br>
DAG를 사용하여 추출한 원시 데이터와</br>
Preset 내에서 쿼리를 사용하여 만든 분석 데이터</br>
<strong style="color:salmon">🎄 Charts</strong></br>
원시 데이터에 기초한 차트들과 조인 쿼리로 도출된 차트들을 대시보드에 최종적으로 게시

<div style="text-align:center; border:1px solid black">

**기본 차트**</br>
![alt text](basic_charts.gif)
</br>
**분석 차트**</br>
![alt text](joined_charts.gif)
</div>

<span style="color: black; display: block; background-color: lightgrey;">

### Requirements

</span>
👉🏽 DB 접근 권한</br>
👉🏽 Preset 계정 또는 Superset 이미지</br>
👉🏽 도커 최신 버전</br>
👉🏽 'dags' 폴더가 있는 프로젝트 폴더 셋업</br>
👉🏽 <a href="https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html">에어플로우 설치용 docker-compose.yaml</a></br>
👉🏽 Superset과 Airflow를 병행하는 경우 쎈 컴퓨터 😁

<span style="color: black; display: block; background-color: lightgrey;">

### Installation

</span>
<div style="color: salmon">
☝🏽 <span style="background-color: black">echo "AIRFLOW_UID=$(id -u)" > .env</span></br>
✌🏽 <span style="background-color: black">docker compose up airflow-init</span></br>
🤟🏽 <span style="background-color: black">docker compose up</span></br>
</div>

<span style="color: black; display: block; background-color: lightgrey;">

### Usage

</span>

1️⃣ DAG 스크립트에서 스키마 수정</br>
2️⃣ Airflow와 DAG에 connection 정보 입력</br>
3️⃣ DAG 잡 실행 </br>
4️⃣ Preset에서 DB 로드업 및 차트 작성

<span style="color: black; display: block; background-color: lightgrey;">

### Documentation

</span>

[Dashboard](https://c27cdb74.us1a.app.preset.io/superset/dashboard/8/?native_filters_key=dePRFh8jT3k-1BKZDVxzNafS8UWRBPHYAL-HO3onQt7CSZv-Csy5SF3ErebAH2zc)</br>
[Team Notion](https://www.notion.so/30fcf69afcef4918b74e5cc551fd6aaf)

<div style="text-align: center">

### Stack
![Static Badge](https://img.shields.io/badge/preset-%E2%9D%A4-black?style=plastic&logo=apachesuperset&logoColor=%23ffffff)
![Static Badge](https://img.shields.io/badge/Python-3.12-black?style=plastic&logo=python&logoColor=%23ffffff)
![Static Badge](https://img.shields.io/badge/airflow-2.10.2-black?style=plastic&logo=apacheairflow&logoColor=%23ffffff)
![Static Badge](https://img.shields.io/badge/redshift-%E2%9D%A4-black?style=plastic&logo=amazonredshift&logoColor=%23ffffff)
![Static Badge](https://img.shields.io/badge/docker-27.3.1-black?style=plastic&logo=docker&logoColor=%23ffffff)
![Static Badge](https://img.shields.io/badge/git-2.43.0-black?style=plastic&logo=git&logoColor=%23ffffff)

### APIs
![Static Badge](https://img.shields.io/badge/yfinance-0.2.50-black?style=plastic&logo=internetarchive&logoColor=%23ffffff)
![Static Badge](https://img.shields.io/badge/Binance-2.9-black?style=plastic&logo=binance&logoColor=%23ffffff)
![Static Badge](https://img.shields.io/badge/kexim-%E2%9D%A4-black?style=plastic&logo=adminer&logoColor=%23ffffff)

### Contributors
<table>
  <tr>
    <td align="center">
      <a href="https://github.com/hosic2">
        <img src="https://github.com/hosic2.png" alt="김주호" />
      </a>
    </td>
     <td align="center">
      <a href="https://github.com/boolYikes">
        <img src="https://github.com/boolYikes.png" alt="선동원" />
      </a>
    </td>
    <td align="center">
      <a href="https://github.com/bkshin01">
        <img src="https://github.com/bkshin01.png" alt="신보경" />
      </a>
    </td>
    <td align="center">
      <a href="https://github.com/saeson7210">
        <img src="https://github.com/saeson7210.png" alt="심승애" />
      </a>
    </td>
    <td align="center">
      <a href="https://github.com/j-eum">
        <img src="https://github.com/j-eum.png" alt="음정민" />
      </a>
    </td>
    <td align="center">
      <a href="https://github.com/HssngH">
          <img src="https://github.com/HssngH.png" alt="현승현" />
      </a>
    </td>
  </tr>
  <tr>
    <td align="center">
      <a href="https://github.com/hosic2">
        <b>김주호</b>
      </a>
    </td>
     <td align="center">
      <a href="https://github.com/boolYikes">
        <b>선동원</b>
      </a>
    </td>
    <td align="center">
      <a href="https://github.com/bkshin01">
        <b>신보경</b>
      </a>
    </td>
    <td align="center">
      <a href="https://github.com/saeson7210">
        <b>심승애</b>
      </a>
    </td>
    <td align="center">
      <a href="https://github.com/j-eum">
        <b>음정민</b>
      </a>
    </td>
    <td align="center">
      <a href="https://github.com/HssngH">
        <b>현승현</b>
      </a>
    </td>
  </tr>
  <tr>
    <td align="center">
      <span>Crypto-currency DAG</br>Dashboard</br>Docker</br>Git</span>
    </td>
    <td align="center">
      <span>World SM Indices DAG</br>Dashboard</br>Git</span>
    </td>
    <td align="center">
      <span>ETF DAG</br>Dashboard</span>
    </td>
    <td align="center">
      <span>KRW based exchange rate DAG</br>Dashboard</span>
    </td>
    <td align="center">
      <span>Forex DAG</br>Dashboard</span>
    </td>
    <td align="center">
      <span>Commodities DAG</br>Dashboard</span>
    </td>
  </tr>
</table>

### License
⚖ Apatji License 2.0

</div>

