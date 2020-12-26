# spark-skew-join-bench

spark를 사용하여, Partial-Iterative-BroadcastJoin을 소개하고 sort merge join과 비교한다. 
-1. iterative-broadcast-join[https://github.com/godatadriven/iterative-broadcast-join]-
1. Partial-Iterative-BroadcastJoin
2. Sort Merge Join without Spark AQE
3. Sort merge Join with Spark AQE


## TODO
- skew data생성을 좀더 수식에 근거해서 생성하도록 생성 알고리즘 변경
- dataFrame보다 dataSet으로 spark계산이 되도록 수정
- local spark로 진행결과, skew데이터임에도 일반 join의 성능이 월등힌 높음, iterate하는 과정에서 생기는 
io가 성능 저하의 원인으로 보임, 이를 수정하여 iterate중간에 io를 제거

## 서

참조
https://m.blog.naver.com/PostView.nhn?blogId=j3man1&logNo=20024527361&proxyReferer=https:%2F%2Fwww.google.com%2F
