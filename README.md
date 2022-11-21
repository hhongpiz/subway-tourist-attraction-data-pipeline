# 날씨에 따른 서울시 관광지 혼잡도 예측 서비스
![image](https://user-images.githubusercontent.com/108858076/202995877-19ec88b6-3d54-4c56-8be0-0a9089a6681e.png)

### [기획배경]
- 성향, 상황, 날씨에 따라 약속장소 선호도 차이가 존재
- 코로나 사회적 거리두기 전면 해지 이후 국내여행 및 근교 나들이 원하는 관광객들이 많아졌지만 여전히 코로나 감염불안감 존재 -> 밀집 지역 비선호 인구 존재
- 날씨, 기후는 관광객의 의사결정에 큰 영향을 준다. (출처:한국형 관광기후지수(KTCI)의 개발에 관한 연구)
- 기후변화와 기상요인이 관광전체에 미치는 영향이 매우 크다 (출처:관광산업의 경쟁력 제고를 위한 날씨경영과 날씨 마케팅 전략 연구)
- 날씨에 따라 유동인구를 예측하여 해당 지역의 관광지와 맛집을 추천하여 약속장소를 잡을 때 참고할 수 있는 웹서비스
----------------------------
### [기획의도]
- 코로나19여파로 정체되어있던 서울시내 관광지 및 맛집 활성화
- 날씨 예보로 지하철 역별 혼잡도를 예측하여 소비자에게 선택지 제공
- 타겟층: 해당지역의 인기있는 장소를 알고 싶은 관광객과 혼잡한 곳을 피하고 싶은 관광객
----------------------------
### [사용데이터]
1. 날씨및 대기환경 데이터
- 기상청 종관기상관측(ASOS)
- 기상청 실시간 단기예보
- 서울시 열린데이터광장 권역별 대기환경현황
- 에어코리아 대기환경정보
2. 지하철 유동인구 데이터
- 서울교통공사 1~8호선 날짜시간대별 데이터
- 공공데이터포털 역주소 및 좌표
- 공공데이터포털 행정구정보
3. 관광지 및 맛집 데이터
- 한국 관광데이터랩 자치구별 관광지 및 맛집 정보 데이터
- 네이버 지도 관광지 근처역정보
- 카카오api 좌표정보
<img width="660" alt="image" src="https://user-images.githubusercontent.com/108858076/202991114-8b13e8a2-e161-440c-8ff3-e7c00a319c55.png">

- 기간 : 코로나 이전 1년을 포함한 3개년 데이터(19~21년)
- 시간대 : 활동시간을 고려한 오전10시 ~ 오후10시
- 지역 : 서울시내 25개 자치구
----------------------------

### [데이터 파이프라인]
- WorkFlow

![image](https://user-images.githubusercontent.com/108858076/202995759-cd09db3e-7622-4f62-8cef-dd59e84927f6.png)

- [DATA WAREHOUSE](https://www.erdcloud.com/d/EQoAZtnumsy6ujTr6)
<img width="660" alt="image" src="https://user-images.githubusercontent.com/108858076/202994700-d3df0a17-b4bc-4a17-81a2-5e42bd29cca8.png">

- [DATA MART](https://www.erdcloud.com/d/wykFB2PqM3ZGjiANF)
<img width="660" alt="image" src="https://user-images.githubusercontent.com/108858076/202994722-80795d84-9524-4c8c-abf8-9c056d9e1fbf.png">

- airflow 배치작업
<img width="300" alt="image" src="https://user-images.githubusercontent.com/108858076/202995381-7b348174-5810-4dec-821e-dbf8b36b561e.png">
<img width="660" alt="image" src="https://user-images.githubusercontent.com/108858076/202995332-a023a415-2f33-458f-b4c1-174a2de8dac6.png">


----------------------------
[기술스택]
- 협업툴
<img src="https://img.shields.io/badge/docker-blue?style=for-the-badge&logo=docker&logoColor=white">
<img src="https://img.shields.io/badge/slack-blue?style=for-the-badge&logo=slack&logoColor=white">
<img src="https://img.shields.io/badge/docker-blue?style=for-the-badge&logo=docker&logoColor=white">
<img src="https://img.shields.io/badge/docker-blue?style=for-the-badge&logo=docker&logoColor=white">
<img src="https://img.shields.io/badge/docker-blue?style=for-the-badge&logo=docker&logoColor=white">
<img src="https://img.shields.io/badge/docker-blue?style=for-the-badge&logo=docker&logoColor=white">
<img src="https://img.shields.io/badge/docker-blue?style=for-the-badge&logo=docker&logoColor=white">
<img src="https://img.shields.io/badge/docker-blue?style=for-the-badge&logo=docker&logoColor=white">
<img src="https://img.shields.io/badge/docker-blue?style=for-the-badge&logo=docker&logoColor=white">
<img src="https://img.shields.io/badge/docker-blue?style=for-the-badge&logo=docker&logoColor=white">
<img src="https://img.shields.io/badge/docker-blue?style=for-the-badge&logo=docker&logoColor=white">




- [Trello](https://trello.com/b/3TLjdRm3/%EA%B7%B8%EB%9E%98%EC%84%9C-%EC%96%B8%EC%A0%9C-%EB%96%A0%EB%82%98%EC%A1%B0)

##### [웹서비스](https://github.com/Beigee/weather_subway_django.git)

### [개선점 및 향후계획]
1. 데이터 확보
- 실시간 유동인구 데이터 : 비용 문제로 인해 실시간 데이터 확보 실패 데이터 수급이 가능하다면 정확한 혼잡도 예측 가능
- 관광지/맛집 데이터 : 한국 관광데이터랩에서 제공하는 관광지/맛집 정보 외에더 다양한 데이터 반영
2. 서비스 확장
- 서울시에서 전국으로 지역 확장
- SNS나 리뷰 등의 로그 데이터를 수집하여 사이트 내에서 인기있는 관광지나 맛집의 순위 제공
- 앱 내 사용 언어를 다양화 하여 내국인 뿐만 아니라 외국인에게도 제공

### 느낀점
- 홍효정:이번 프로젝트는 특히 비전공자만으로 구성되어있어 적절한 주제를 선정하는 것부터 ETL, 모델링, 웹서비스를 구현하는 것까고민도 많았고 난관도 많았습니다. 모두 처음 사용하는 기술도 많았지만 팀원들끼리 서로 배워가며 협업한 결과 프로젝트를 성공적으로 완성할 수 있었다고 생각합니다. 전공수업동안 배웠던 것을 녹여내려고 많이 노력했고 결과적으로 이번 프로젝트를 통해 많은 것을 얻어갈 수 있었습니다.   
- 유승종:2차 프로젝트에서 했던 데이터 엔지니어링 과정을 한 번 더 진행하면서 이해도를 높일 수 있었으며, 서비스를 만드는 전반적인 흐름을 파악할 수 있었습니다.  또한 두 전공이 합심하여 프로젝트를 하면서 협업을 하는 법을 많이 배우게 됐습니다. 처음부터 끝까지 난관이 많았지만 좋은 팀원들을 만나 끝까지 잘 마무리 할 수 있었습니다.
- 김민석:이것만하면 끝난다는 생각때문인지 1,2차때보다는 말을 좀 더 했던 것 같습니다. 처음 멘토링때 저희 걱정해주시던 멘토님들이 점점 저희 잘해냈다고 해주시고 강사님들도 말씀해주시니 거기에 힘이나 다들 더 열심히 했던게 아닐까하는 생각이 들었고 열심히 해서 좋은 결과가 나왔던 것 같아 좋은 경험이라고 생각합니다.
- 이중훈:처음에 주제를 정하고 서비스 구현까지 할 수 있을까 걱정이 많았는데, 프로젝트 초반부터 끝날 때까지 각각DS,DE 팀원끼리 정말 문제없이 소통을 많이해서 잘 마무리한 것 같습니다. 좋은 아이디어를 많이 내주시고, 맡은 역할에 정말 최선을 다 해주셔서 팀원분들에게 정말 감사드리고 힘들었지만 좋은 경험이었다고 생각합니다.
- 박준수:분대장 이후로 팀장을 처음 해봤는데 프로젝트를 이끈다는 것이 여간 어려운일이 아니었다. 그렇지만 팀원 분들이 잘 따라주셔서 원활한 프로젝트가 된 것같고 배운 것 이상으로 많은 것들을 경험할 수 있어서 좋았습니다
- 김태현:교육과정 중 DS 와 DE 의  처음이자 마지막인 공동 프로젝트를 진행하면서 많은 이슈가 있었지만 팀원들간의 커뮤니케이션으로 잘 해결 할수있었고 성공적인 프로젝트라고 생각하며 좋은 경험이되었던거 같습니다.
