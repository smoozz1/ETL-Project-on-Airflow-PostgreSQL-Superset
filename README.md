# ETL-Project-on-Airflow-PostgreSQL-Superset  
Проект на базе Airflow, PostgreSQL и Superset, развернутый локально с помощью Docker Compose. В проекте реализован Extract - Transform - Load пайплайн, использующий библиотеки Python.  
<img width="1330" height="818" alt="image" src="https://github.com/user-attachments/assets/a4ba344e-4ee8-4092-95e2-76c17e8037b4" />
  
![вакансии-аналитики-2025-08-20T09-56-20 695Z](https://github.com/user-attachments/assets/1b22f0dc-087d-465c-b8b7-fba738532ad3)

## Запуск окружения

### 1. Для запуска потребуется Docker Compose, если его нет - [скачиваем](https://docs.docker.com/compose/install/)  

### 2. В директории репозитория запускаем инициализацию бд

```bash
docker compose up db
```

### 3. Далее запускаем локальное окружение

```bash
docker compose up
```

*без ключа `-d`, так как в логах будет отображён пароль от Airflow*  

### 4. Авторизуемся  
[**Airflow**](http://localhost:8000/)   
логин: admin  
пароль: *из консоли* (<kbd>CTRL</kbd> + <kbd>F</kbd> ; <kbd>CTRL</kbd> + <kbd>V</kbd> `password`)  

[**Superset**](http://localhost:8088/)  
логин: admin  
пароль: admin  

## Работа с окружением    
### 1. Копируем дамп файл в контейнер  
`docker cp ./data.dump postgres_container:/tmp/data.dump`

### 2. Подгружаем его в БД и тут же удаляем из контейнера  
`docker exec postgres_container bash -c "pg_restore -U db_user -d db /tmp/data.dump && rm -f /tmp/data.dump"`  

### 3. Загружаем дашборд в суперсет
<img width="883" height="125" alt="image" src="https://github.com/user-attachments/assets/ef5b715d-7503-45da-84bb-ae338cb9362d" />  

*Здесь выбираем файл, жмем **IMPORT**, вводим пароль от БД `db_password` и снова жмем **IMPORT***
<img width="823" height="431" alt="image" src="https://github.com/user-attachments/assets/4be50b5c-8430-4260-a7cf-0d414edc128a" />

### 4. Любуемся
   
![вакансии-аналитики-2025-08-20T09-56-20 695Z](https://github.com/user-attachments/assets/1b22f0dc-087d-465c-b8b7-fba738532ad3)  

### 5. Актуализируем данные, запуская DAG-процесс в Airflow

<img width="837" height="428" alt="image" src="https://github.com/user-attachments/assets/15ea79ed-76bc-43af-80ae-e91d5c255478" />

## Как оно работает

### Окружение

`docker compose up` собирает локальное окружение по конфигу, который находится в файле `docker-compose.yaml`  

В конфиге прописаны 5 контейнеров:
- PostgreSQL
- Apache Airflow

❗ Для запуска DAG-процесса, описанного в `./airflow/dags/hh_scraper.py`, необходима установка кастомного билда Airflow (`./airflow/Dockerfile`) с браузером, веб-драйвером и библиотеками Python.

- Apache Superset
- инициализатор Superset’а 
- redis для кеширования данных из Superset

Контейнеры соединены между собой сетью `my-network`. Для метаданных Airflow и Superset созданы соответственные базы данных в PostgreSQL.  
Для веб-интерфейсов Airflow и Superset назначены локальные порты :8000 и :8088.

### DAG (ETL pipeline)  

Файл DAG — `./airflow/dags/hh_scraper.py`. 

В коде Extract - Transform - Load процесс разбит по функциям.  

#### Extract (scraping_data)

- запуск веб-драйвера из библиотеки *selenium*
- драйвер переходит на страницу `https://hh.ru`,  
находит поисковую строку и вводит `дата аналитик`,  
нажимает <kbd>Enter</kbd>, закрывает предложение зарегистрироваться <kbd>ESCAPE</kbd>  
- далее драйвер проходит по каждой странице (1-40) с поискового запроса и собирает названия вакансий, организаций и ссылки
- таск завершается сбором информации о вакансии с каждой ссылки, полученной на предыдущем этапе

**Инфа:**
- зарплата
- рейтинг работодателя
- количество оценок работодателя
- требуемый опыт
- график работы
- формат работы
- требуемые навыки
- ближайшие станции метро
- дата публикации  

#### Transform (preprocessing_data)

- вычет налогов из зарплаты,
перевод в ₽ с учётом курса валют ЦБ РФ (если исходная зарплата указана не в рублях)
- категорирование по названию вакансии на грейды и направления деятельности
- создание дополнительной таблицы с разнесенными строками навыков, формата работы, графика работы и метро

#### Load (load_data)

- подключение к БД
- создание схемы `dev_v1` и таблиц `scraper_hh`, `scraper_hh_extra`
- импорт данных в таблицы с возможностью обновления при наличии конфликтов по `id`

## P.S.
При желании проект можно адаптировать под другие профессии, скорректировав категорирование и ключевые навыки.
