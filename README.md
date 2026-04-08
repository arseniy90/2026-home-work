# 2026-home-work 

[![Build Status](https://github.com/vk-edu-distrib-compute/2026-home-work/actions/workflows/gradle-build.yaml/badge.svg)](https://github.com/vk-edu-distrib-compute/2026-home-work/actions/workflows/gradle-build.yaml/badge.svg)
[![Code Style Check](https://github.com/vk-edu-distrib-compute/2026-home-work/actions/workflows/gradle-code-style.yaml/badge.svg)](https://github.com/vk-edu-distrib-compute/2026-home-work/actions/workflows/gradle-code-style.yaml)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/fdb601d406384215a5a37372cc3cf06a)](https://app.codacy.com/gh/vk-edu-distrib-compute/2026-home-work/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_grade)

Домашние задание курса "Распределенные вычисления" 2026 года

## Задание 1. HTTP API + хранилище (deadline 1 неделя)
### Fork
[Форкните проект](https://help.github.com/articles/fork-a-repo/), склонируйте и добавьте `upstream`:
```
$ git clone git@github.com:<username>/2026-home-work.git
Cloning into '2026-home-work'...

...

$ git remote add upstream git@github.com:vk-edu-distrib-compute/2026-home-work.git
$ git fetch upstream
From github.com:vk-edu-distrib-compute/2026-home-work
 * [new branch]      master     -> upstream/master
```

### Test
Так можно запустить тесты:
```
$ ./gradlew check
```
### Run
А вот так -- сервер:
```
$ ./gradlew run
```

### Code style checks
```
$ ./gradlew codeStyleChecks
```

### Develop
Откройте в IDE -- [OpenIDE](https://openide.ru/download/) нам будет достаточно.

**ВНИМАНИЕ!** При запуске тестов или сервера в IDE необходимо передавать Java опцию `-Xmx128m`. 

В своём Java package `company.vk.edu.distrib.compute.<username>` реализуйте интерфейс [`KVService`](src/main/java/company/vk/edu/distrib/compute/KVService.java) и поддержите следующий HTTP REST API протокол:

* HTTP `GET /v0/status` -- `200` в нормальной ситуации, `503` в случае проблем.
* HTTP `GET /v0/entity?id=<ID>` -- получить данные по ключу `<ID>`. Возвращает `200 OK` и данные или `404 Not Found`.
* HTTP `PUT /v0/entity?id=<ID>` -- создать/перезаписать (upsert) данные по ключу `<ID>`. Возвращает `201 Created`.
* HTTP `DELETE /v0/entity?id=<ID>` -- удалить данные по ключу `<ID>`. Возвращает `202 Accepted`.

1. Сделать наследника [`KVServiceFactory`](src/main/java/company/vk/edu/distrib/compute/KVServiceFactory.java) в пакете со своим именем/ником.
2. Ваша реализация интерфейса [`KVService`](src/main/java/company/vk/edu/distrib/compute/KVService.java), возвращаемая из `KVServiceFactory`, должна запускать [HttpServer из JDK](https://docs.oracle.com/en/java/javase/25/docs/api/jdk.httpserver/com/sun/net/httpserver/HttpServer.html).
3. Ваш `KVService` должен работать с вашей же реализацией интерфейса [`Dao`](src/main/java/company/vk/edu/distrib/compute/Dao.java) и делегировать непосредственную работу с данными хранилища.
4. В минимальной реализации `Dao` достаточно хранить данные в памяти. Для первого этапа `T` в `Dao` будет `byte[]`.
5. Добавить своего наследника `KVServiceFactory` в поле [`KVServiceFactoryArgumentsProvider.factories`](src/integrationTest/java/company/vk/edu/distrib/compute/KVServiceFactoryArgumentsProvider.java)

Продолжайте запускать тесты и исправлять ошибки, не забывая [подтягивать новые тесты и фиксы из `upstream`](https://help.github.com/articles/syncing-a-fork/). 
Если заметите ошибку в `upstream`, заводите баг и присылайте pull request ;)

### Report
Когда всё будет готово, присылайте pull request со своей реализацией на review. Не забывайте **отвечать на комментарии в PR** и **исправлять замечания**!

### Bonus tasks

#### Persistent Dao

Cделать `Dao`, хранящие данные на файловой системе. Если сделали так, напишите коммент об этом к своему PR'у.

#### Load test

Проведите нагрузочное тестирование с помощью [wrk](https://github.com/giltene/wrk2) в **одно соединение**:
* `PUT` запросами на **стабильной** нагрузке (`wrk` должен обеспечивать заданный с помощью `-R` rate запросов) наполните базу
* `GET` запросами на **стабильной** нагрузке по **наполненной** БД

1. wrk2 можно собрать из исходников, взять в вашем дистре линукса или взять готовый докер, например [тут](https://hub.docker.com/r/haydenjeune/wrk2)
2. Для докера инструкция запуска wrk есть прямо по ссылке, помните, что в случае докера надо указать IP вашего сетевого интерфейса созданного докером (обычно это `docker0`), а не просто localhost
3. Запускайте со след. параметрами `-t1 -c1 -R200 -d30s --latency -s /data/request.lua`
4. Сохраните выведенную статистику (начинается после строчки Detailed Percentile spectrum:) в файл и отобразите [здесь](https://hdrhistogram.github.io/HdrHistogram/plotFiles.html)
5. Скрины графиков (PUT, GET, кнопка Export Image на сайте) надо приложить к вашему PR'у

## Задание 2. Шардирование

Реализуем горизонтальное масштабирование через поддержку кластерных конфигураций, состоящих из нескольких узлов, взаимодействующих друг с другом через реализованный HTTP API.

### Sharding. Test

Так можно запустить тесты:
```
./gradlew check
```

### Sharding. Run

Запустить сервер в режиме кластера:
```
./gradlew run --args="cluster"
``` 

### Sharding. Code style checks

```
/gradlew codeStyleChecks
```

### Sharding. Develop

* Кластер распределяет ключи между узлами детерминированным образом.
* В кластере хранится только одна копия данных.
* Нода, получившая запрос, проксирует его на узел, отвечающий за обслуживание соответствующего ключа.
* Таким образом, общая ёмкость кластера равна суммарной ёмкости входящих в него узлов.
* Реализуйте один из алгоритмов распределения данных между узлами, например, consistent hashing, rendezvous hashing.

* Используйте свою реализацию KVCluster и KVClusterFactory
* В качестве хранилища можете либо переиспользовать созданные в первом задании, либо добавить реализацию dao с использованием h2

### Sharding. Report

Когда всё будет готово, присылайте pull request со своей реализацией на review. Не забывайте **отвечать на комментарии в PR** и **исправлять замечания**!

### Sharding. Bonus tasks

#### Sharding. Additional algorithm

Реализуйте ещё один алгоритм распределения данных. Например, если в основном задании сделали через rendezvous hashing, сделайте через consistent hashing и наоборот.

#### Sharding. Load test

* Провести нагрузочное тестирование с помощью wrk2 на распределенный кластер с большим количеством соединений >= 64 
* Запускайте со след. параметрами `-t2 -c100 -R200 -d30s --latency -s /data/request.lua`
* Сравнить с предыдущей (монолитной/не распределенной) версией.
* Результаты и анализ сравнения приложить к PR.
