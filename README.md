![вакансии-аналитики-2025-08-20T09-56-20 695Z](https://github.com/user-attachments/assets/1b22f0dc-087d-465c-b8b7-fba738532ad3)


1. Для запуска потребуется Docker Compose, если его нет - скачиваем  

2. В директории репозитория запускаем инициализацию бд  
```bash
docker compose up db
```

3. Далее запускаем локальное окружение  
```bash
docker compose up
```  
*без приписки -d, так как в логах будет пароль от airflow*  

4. Авторизуемся  
Airflow
логин: admin
пароль: *из консоли* (ctrl + f `password`)
Superset
логин: admin
пароль: admin  

5. Можем работать с окружением  

6. Подгружаем данные  
Копируем дамп в контейнер  
 - `docker cp ./data.dump postgres_container:/tmp/data.dump`  
Подгружаем его в БД и тут же удаляем  
 - `docker exec postgres_container bash -c "pg_restore -U db_user -d db /tmp/data.dump && rm -f /tmp/data.dump"`  
Загружаем дашборд в суперсет
<img width="883" height="125" alt="image" src="https://github.com/user-attachments/assets/ef5b715d-7503-45da-84bb-ae338cb9362d" />
Вводим пароль от БД `db_password`  
<img width="823" height="431" alt="image" src="https://github.com/user-attachments/assets/4be50b5c-8430-4260-a7cf-0d414edc128a" />
8. Любуемся
   
![вакансии-аналитики-2025-08-20T09-56-20 695Z](https://github.com/user-attachments/assets/1b22f0dc-087d-465c-b8b7-fba738532ad3)  

9. Чтобы актуализировать данные запускаем DAG-процесс

<img width="837" height="428" alt="image" src="https://github.com/user-attachments/assets/15ea79ed-76bc-43af-80ae-e91d5c255478" />
