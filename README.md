# FoxbatDB

������K-V���ݿ⣺֧������ACID������Redis����

## 1 ��Ҫ�ص�

### 1.1 ����

* ��ƽ̨
* ��д���ܽӽ�Redis
* crash-safe
* ֧������ACID
* ֧��keyǰ׺ģ��ƥ��
* �����ָ������ݱ��ݻ�Ǩ�Ʋ�����
* ʹ��RESP 3Э����ͻ���ͨ�ţ�����Redis�����ֱ��ʹ�����е�Redis�ͻ���

### 1.2 ����

* ����key��ά�����ڴ���

## 2 ��Ƹ���

### 2.1 �ڴ������ṹ

FoxbatDB�ڴ������ṹ�����ֵ�����Trie��������ʵ��key��ǰ׺ģ������ƥ�䣻
���⣬Ϊ�˽����ͨTrieռ���ڴ��������⣬���û���burst-trieԭ��ʵ�ֵ�HAT-trie[<sup>[1]</sup>](#refer-anchor-1)
��Ϊʵ�ʵ��ڴ������ṹ��  
![Hat-trie�ṹ](https://tessil.github.io/images/hat-trie/hat_trie_hybrid.png)

### 2.2 ���̴洢�ṹ

FoxbatDB�������ݴ洢ʹ��Ԥд��־��Write Ahead Log����
������־�ļ�Ϊֻ׷��д�루append-only���ļ����Ի�ýϼѵ�д�����ܡ�
�洢���Բο�Bitcask[<sup>[2]</sup>](#refer-anchor-2)ʵ�֡�
![FoxbatDB���̴洢](images/data.png)

### 2.3 ����

FoxbatDB�������ΪRedis�����ACID��չ����Redis���������������֤ACID��

* ԭ���ԣ�Atomic�������������д������һ������ִ���ڼ䷢�����󣬽��Զ��ع�
* �����ԣ�Isolation����ͨ��Watch�ֹ�������ʵ�֣���Redisһ��
* �־��ԣ�Durability�������������д����������ִ���ڼ佫����ִ��״̬��д������һ��־û��������ϣ�File Record�ṹ�е�Tx
  State��
* һ���ԣ�Consistency�����ڼ�����ʷ����ʱ��ͨ������¼������ִ��״̬����֤��ʹ����ִ���ڼ�ϵͳ����Ҳ�ܽ����ݿ�ָ���ȷ��״̬

### 2.4 �����Ż�����

* CPU�����Ż�����ҪƵ��ʹ�õ����ݽṹ�����ڴ������CPU L1�����д�С
* �ڴ�ʹ���Ż�����Ƶ������/���ٵ����ݽṹ��ʵ����ר�õĶ���أ��Լ����ڴ���Ƭ�Ͷ�̬�ڴ����/�ͷŵĿ���

## 3 ���ٿ�ʼ

### 3.1 ��Դ�빹��

* Linux/Mac OS

```shell
mkdir build
cd build
cmake ..
make
```

* Windows  
  [Micoosoft�ٷ��ĵ�������CMake��Ŀ](https://learn.microsoft.com/en-us/cpp/build/cmake-projects-in-visual-studio?view=msvc-170)

## 4 ���ܷ���

### 4.1 ��������

* ����ϵͳ��Ubuntu 22.04
* ��������gcc 11.4.0������O3�Ż�
* ��������  
  ![��������](images/machine.png)

### 4.2 ��д����

* ʹ��[google/benchmark](https://github.com/google/benchmark)�������ܲ���
* �����ļ�Ϊtest/benchmark/benchmark.cc  
  ![benchmark](images/benchamrk.png)

### 4.3 ��Redis�Ա�

* Redis�������þ�����Ĭ�ϣ��汾Ϊ7.2.2
* ��ÿһ�����������100�Σ�ͳ�ƺ�ʱƽ��ֵ
* ���Խű�Ϊtest/benchmark/compare_redis.py
  ![��Redis�ԱȽ��](images/benchmark_redis.png)

## �ο�

<div id="refer-anchor-1"></div>

[1] [Askitis N, Sinha R. HAT-trie: a cache-conscious trie-based data structure for strings[C]//ACSC. 2007, 97: 105.](https://d1wqtxts1xzle7.cloudfront.net/65965420/CRPITV62Askitis-libre.pdf?1615412661=&response-content-disposition=inline%3B+filename%3DHAT_Trie_A_Cache_Conscious_Trie_Based_Da.pdf&Expires=1705206514&Signature=N5Zff-G1FTsDEfjE5-RwT5J9nSA~i89PIBE2SXBjFrQ-goCLiRHtcB7XbvpMxJsBpkZ5JvR75WwiMsAzVwOAr85FVoFwsICUmwZ-EFoKlzeKml~QxzDD7X8MKFPk3-8OP5RqycwcL~9-KoT8J-JUkoTRX-5ZV9qBX70LEOqI6E8VMwHrER05zf7VSQKwmEnVLYlV9imhy0InxCKLc-4e50xrIt4D96b0QZNqJf~dnsrxpSdVz9mfLQU8QYNnOvgCH77utQhCRvh~jL~GVKdGcWxYO0Z3WcndJ5GGtpvCDSmEaG4u-beoMkRKosDFX~v0iis7UNlO8Uh0hrPSWPusdQ__&Key-Pair-Id=APKAJLOHF5GGSLRBV4ZA)

<div id="refer-anchor-2"></div>

[2] [Sheehy J, Smith D. Bitcask: A log-structured hash table for fast key/value data[J]. Basho White Paper, 2010.](https://riak.com/assets/bitcask-intro.pdf)