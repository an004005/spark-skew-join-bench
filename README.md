# spark-skew-join-bench

spark를 사용하여, skew data join을 수행하는 방법에 대해서 밴치마크를 수행한다.
1. iterative-broadcast-join[https://github.com/godatadriven/iterative-broadcast-join]
2. Salt Join
3. Sort Merge Join without Spark AQE
4. Sort merge Join with Spark AQE


## TODO
- skew data생성을 좀더 수식에 근거해서 생성하도록 생성 알고리즘 변경
- dataFrame보다 dataSet으로 spark계산이 되도록 수정


참조
https://m.blog.naver.com/PostView.nhn?blogId=j3man1&logNo=20024527361&proxyReferer=https:%2F%2Fwww.google.com%2F
