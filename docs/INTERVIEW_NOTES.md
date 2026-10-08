

Interview Notes - Real-Time Streaming Lakehouse
===

## Day 1 - Environment Setup

### Objective

Set up the development environment and initialize version control before
building the data platform.

### Environment

* Windows 11
* Python 3.12.10
* pip 26.2.1
* Docker 29.8.0
* Docker Compose v5.5.1
* Git 2.56.0
* Docker Desktop using WSL2

### What We Verified

Docker Engine is running successfully.

Docker Compose is available for running multiple services together.

Python virtual environments are available through the built-in venv module.

The direct pip.exe command is blocked by Windows Application Control,
but pip works correctly through:

python -m pip

We did not bypass the Windows security policy.

### Why Git Was Initialized First

The project is tracked from the beginning so that the development history
shows how the system was built incrementally.

### Why Docker

The project contains multiple services with different dependencies.
Docker allows these services to run in isolated and reproducible environments.

Docker Compose will allow us to define and manage the multi-container
environment from configuration.

### Initial Architecture

Producer
|
v
Kafka
|
v
Spark Structured Streaming
|
v
Bronze
|
v
Silver
|
v
Gold
|
v
Analytics / Dashboard

### Git Commands Learned

git init

* Initializes a new local Git repository.

git status

* Shows the current working-tree and staging-area state.

git add .

* Stages eligible changes for the next commit.

.gitignore

* Prevents selected files such as secrets, virtual environments,
caches, logs, and generated data from being tracked.

### Interview Talking Point

I initialized Git before starting development so that each major
implementation stage could be tracked through meaningful commits.
I also containerized the platform because it consists of multiple
services with different dependencies and configurations.

## Problems Encountered

### pip.exe blocked

Problem:
Windows Application Control blocked the standalone pip.exe.

Solution:
Verified that pip is available through:

python -m pip --version

Result:
pip 26.2.1 is working.

Important:
We did not attempt to bypass the Windows security policy.

## Next Steps

1. Create the first Git commit.
2. Connect the repository to GitHub.
3. Design the initial data schema.
4. Create Docker Compose infrastructure.
5. Build the streaming producer

## Day 2 - Kafka Streaming Infrastructure
6. 
7. \### Objective
8. 
9. Build the first working streaming layer of the lakehouse by deploying
10. Kafka, creating a trade-events topic, producing JSON events, and
11. consuming them back from Kafka.
12. 
13. \### Kafka Infrastructure
14. 
15. Kafka was deployed using Docker Compose.
16. 
17. Configuration:
18. 
19. \- Kafka image: apache/kafka:4.1.0
20. \- Single broker
21. \- KRaft mode
22. \- Broker port: 9092
23. \- Controller port: 9093
24. \- Persistent Docker volume for Kafka data
25. 
26. \### Why KRaft?
27. 
28. Modern Kafka deployments can use KRaft instead of ZooKeeper.
29. 
30. KRaft allows Kafka to manage its own metadata and controller quorum,
31. reducing the need for a separate ZooKeeper dependency.
32. 
33. For this project, KRaft keeps the local architecture simpler.
34. 
35. \### Kafka Topic
36. 
37. Created:
38. 
39. trades
40. 
41. Configuration:
42. 
43. \- 3 partitions
44. \- Replication factor: 1
45. \- Single broker
46. 
47. \### Why Partitions?
48. 
49. Partitions allow Kafka to distribute records and provide parallelism.
50. 
51. Kafka guarantees ordering within a partition, but not globally across
52. all partitions.
53. 
54. \### Why Replication Factor 1?
55. 
56. The development environment uses a single Kafka broker.
57. 
58. A production deployment would normally use multiple brokers and a
59. higher replication factor for fault tolerance.
60. 
61. \### Python Producer
62. 
63. Created:
64. 
65. producer/producer.py
66. 
67. Dependency:
68. 
69. kafka-python==2.2.15
70. 
71. The producer generates simulated financial trade events every two
72. seconds.
73. 
74. Each event contains:
75. 
76. \- event\_id
77. \- event\_time
78. \- symbol
79. \- price
80. \- quantity
81. \- side
82. \- source
83. 
84. \### Serialization
85. 
86. The Python dictionary is serialized into JSON and then UTF-8 encoded
87. before being sent to Kafka.
88. 
89. Flow:
90. 
91. Python dictionary
92. &#x20;   |
93. &#x20;   v
94. JSON
95. &#x20;   |
96. &#x20;   v
97. UTF-8 bytes
98. &#x20;   |
99. &#x20;   v
100. Kafka
101. 
102. Kafka transports records as bytes, so serialization is required.
103. 
104. \### Key-Based Partitioning
105. 
106. The producer sends the stock symbol as the Kafka message key.
107. 
108. Example:
109. 
110. producer.send(
111. &#x20;   KAFKA\_TOPIC,
112. &#x20;   key=trade\["symbol"].encode("utf-8"),
113. &#x20;   value=trade,
114. )
115. 
116. This causes records with the same key to be routed consistently to
117. the same partition when the partition count and partitioning strategy
118. remain unchanged.
119. 
120. Observed mapping during testing:
121. 
122. \- AAPL -> partition 0
123. \- MSFT -> partition 0
124. \- GOOGL -> partition 1
125. \- NVDA -> partition 2
126. \- AMZN -> partition 2
127. 
128. Multiple keys can share the same partition.
129. 
130. \### Why Use Symbol as the Key?
131. 
132. The trading pipeline may need to preserve the order of events for a
133. particular stock symbol.
134. 
135. Kafka provides ordering within a partition.
136. 
137. Using symbol as the key allows all events for the same symbol to be
138. routed to the same partition, allowing their relative order to be
139. preserved.
140. 
141. \### Kafka Offsets
142. 
143. Each record receives an offset within its partition.
144. 
145. Example:
146. 
147. Partition 0:
148. offset 50
149. offset 51
150. offset 52
151. 
152. Offsets are scoped to a partition and are not globally unique across
153. the topic.
154. 
155. Consumers use offsets to track their position in the Kafka log.
156. 
157. \### Consumer Test
158. 
159. Used Kafka's console consumer with:
160. 
161. docker exec -it streaming-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic trades --from-beginning
162. 
163. The consumer successfully read events that had already been produced.
164. 
165. This demonstrated Kafka's persistent log and replay capability.
166. 
167. \### Commands Used
168. 
169. Start Kafka:
170. 
171. docker compose up -d
172. 
173. Check services:
174. 
175. docker compose ps
176. 
177. Create topic:
178. 
179. docker exec streaming-kafka /opt/kafka/bin/kafka-topics.sh --create --topic trades --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
180. 
181. List topics:
182. 
183. docker exec streaming-kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
184. 
185. Describe topic:
186. 
187. docker exec streaming-kafka /opt/kafka/bin/kafka-topics.sh --describe --topic trades --bootstrap-server localhost:9092
188. 
189. Run producer:
190. 
191. python producer/producer.py
192. 
193. Consume events:
194. 
195. docker exec -it streaming-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic trades --from-beginning
196. 
197. \### Problem Encountered
198. 
199. The topic creation command was executed twice.
200. 
201. The second attempt returned:
202. 
203. TopicExistsException: Topic 'trades' already exists.
204. 
205. This was not a system failure. It confirmed that the topic had already
206. been successfully created.
207. 
208. \### Day 2 Interview Talking Point
209. 
210. I containerized a single-node Kafka cluster using Docker Compose and
211. KRaft mode. I created a three-partition trades topic and built a Python
212. producer that generates JSON trade events. I initially tested automatic
213. partitioning and then introduced the stock symbol as the Kafka message
214. key so events for the same symbol consistently route to the same
215. partition. I verified the pipeline using Kafka's console consumer and
216. confirmed that previously produced events could be replayed using their
217. stored offsets.
218. 
219. \### Key Interview Questions
220. 
221. Q: Why Kafka?
222. 
223. A: Kafka decouples producers and consumers and provides durable event
224. storage, buffering, replayability, scalability, and partition-based
225. parallelism.
226. 
227. Q: What is a Kafka topic?
228. 
229. A: A topic is a logical stream of records. Topics are divided into
230. partitions for scalability and parallel processing.
231. 
232. Q: What is a partition?
233. 
234. A: A partition is an ordered append-only log within a Kafka topic.
235. Kafka guarantees ordering within a partition.
236. 
237. Q: What is an offset?
238. 
239. A: An offset identifies the position of a record within a Kafka
240. partition and allows consumers to track and resume their progress.
241. 
242. Q: Why use a message key?
243. 
244. A: A key can control partition assignment. Using symbol as the key
245. allows records for the same symbol to consistently go to the same
246. partition and preserves their ordering within that partition.
247. 
248. Q: Why not use replication factor 3?
249. 
250. A: The local environment has only one Kafka broker. Replication requires
251. multiple brokers to provide meaningful fault tolerance. Production
252. would use multiple brokers and an appropriate replication factor.
253. 
254. Q: What is KRaft?
255. 
256. A: KRaft is Kafka's built-in metadata and controller architecture that
257. removes the need for ZooKeeper.
258. 
259. \### Day 2 Result
260. 
261. Producer -> Kafka -> Consumer
262. 
263. The first working streaming ingestion path is complete.

