# Отчет по Лабораторной работе №1


## Ход выполнения работы

### Этап 1. Первоначальная настройка

Склонировал репозиторий Prerequisites

    git clone https://github.com/ssau-data-engineering/Prerequisites.git

Выполнил необходимые пункты из [Порядка запуска](https://github.com/ssau-data-engineering/Prerequisites#%D0%BF%D0%BE%D1%80%D1%8F%D0%B4%D0%BE%D0%BA-%D0%B7%D0%B0%D0%BF%D1%83%D1%81%D0%BA%D0%B0).

### Этап 2. Airflow + EK

Поднял Airflow и EK

    docker compose -f docker-compose.airflow.yaml up --build -d
    docker compose -f docker-compose.elasticsearch.yaml up --build -d

На этом этапе также столкнулся с проблемой входа в веб-интерфейс Airflow (не уидел креды в репозитории Prerequisites), пришлось создавать своего юзера.

Описал граф для Airflow

<p align="center">
  <img width="800" height="300" src="https://github.com/Anteii/ssau-data-engineering-lab-1/blob/main/screenshots/resulting_graph.png"/>
</p>

Поместил файл с описанием графа в контейнер с Airflow (через маунт директорию)

В интерфейсе Airflow выполнил граф (25 раз). Столкнулся на этом этапе с различными проблемами из-за проблем с пониманием документации. Несколько раз переписывал код с нуля (после постепенного ознакомления с Airflow понимал как можно лучше написать), что также ломало пайплайн.

<p align="center">
  <img width="600" height="350" src="https://github.com/Anteii/ssau-data-engineering-lab-1/blob/main/screenshots/total_runs.png"/>
</p>

Задачу <i><b>save_to_elastic</b></i> писал после освоения документации по EK и построения нескольких тестовых дэшбордов.

В Kibana создавал index pattern, загружал данные, после чего рисовал дэшборд.

<p align="center">
  <img width="600" height="300" src="https://github.com/Anteii/ssau-data-engineering-lab-1/blob/main/screenshots/airflow-kibana-panel.png"/>
</p>

На графике изображена зависимость средней оценки вина от цены.

Позже решил для больше наглядности сделал график зависимости средней цены от поставленной оценки.

<p align="center">
  <img width="600" height="300" src="https://github.com/Anteii/ssau-data-engineering-lab-1/blob/main/screenshots/airflow-kibana-panel2.png"/>
</p>

### Этап 3. Apache Nifi

В веб-интерфейсе нарисовал граф

<p align="center">
  <img width="800" height="400" src="https://github.com/Anteii/ssau-data-engineering-lab-1/blob/main/screenshots/nifi-graph.png"/>
</p>

При тестировании столкнулся с тем, что GetFile постоянно считывала файлы из исходной директории, из-за чего забивалась очередь и все зависало (из-за установки в GetGile флага keepSourceFiles)

Полученный график в kibana полностью соответсвует графику полученному после выполнения airflow пайплайна

<p align="center">
  <img width="600" height="300" src="https://github.com/Anteii/ssau-data-engineering-lab-1/blob/main/screenshots/nifi-kibana-panel.png"/>
</p>

### Этап 4. Вывод

Nifi предоставляет удобный визуальный интерфейс построения графа. Как типичное no-code (low-code) решение имеет свои ограничения. Если выходить за рамки простых вещей, требуется более глубокое изучение. Airflow хоть и требует написания кода, но для реализации кастомной логики не требует сторонних вещей: все решается написанием кода на Python.