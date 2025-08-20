from airflow import DAG
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import requests
import json
import re
import psycopg2
from psycopg2.extras import Json
from time import sleep
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup


# настройки для DAG
default_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 8, 5),
    "max_active_runs": 1,
}

schedule_interval = (
    "0 0 * * 1"  # расписание, по которому будет автоматически срабатывать DAG
)

# основные скиллы, которые парсятся из текста описания вакансии
key_skills = [
    "Python",
    "Pandas",
    "NumPy",
    "SciPy",
    "Scikit-learn",
    "Statsmodels",
    "R",
    "dplyr",
    "ggplot2",
    "SQL",
    "Julia",
    "SAS",
    "Spark SQL",
    "PySpark",
    "Tableau",
    "Power BI",
    "Google Looker Studio",
    "Matplotlib",
    "Seaborn",
    "Plotly",
    "Dash",
    "D3.js",
    "Metabase",
    "ggplot2",
    "Redash",
    "Sisense",
    "Qlik",
    "MicroStrategy",
    "SAP Analytics Cloud",
    "Apache Spark",
    "Hadoop",
    "Apache Kafka",
    "Databricks",
    "Snowflake",
    "Google BigQuery",
    "Amazon Redshift",
    "Apache Airflow",
    "dbt",
    "PostgreSQL",
    "MySQL",
    "MongoDB",
    "Redis",
    "Elasticsearch",
    "TensorFlow",
    "PyTorch",
    "XGBoost",
    "LightGBM",
    "H2O.ai",
    "NLTK",
    "spaCy",
    "Hugging Face",
    "Jupyter",
    "VS Code",
    "Excel",
    "Google Sheets",
    "Docker",
    "Git",
    "Bash",
    "Alteryx",
    "SPSS",
]


def categorize_vacancy(title):
    if not isinstance(title, str):
        return "Other"
    title_lower = title.lower()

    if re.search(
        r"(data science|machine learning|ml engineer|ai engineer|deep learning|"
        r"искусственн\w* интеллект|нейронн\w* сет|computer vision|cv инженер|nlp|"
        r"prompt инженер|data scientist|машинн\w* обучен|ml|ai|mlops|ml ops|"
        r"reinforcement learning|исследователь машинн\w* моделей|deepfake|cv разработчик|"
        r"data engineering|data engineer|hadoop|spark|big[ -]?data|etl developer|"
        r"python|tensorflow|keras|pytorch|reinforcement learning|data wrangling|"
        r"scikit-learn|feature engineering|predictive analytics|data analyst|analytics|"
        r"data scientist|data modeling|computational linguistics|data mining)",
        title_lower,
    ):
        return "DS / ML"

    elif re.search(
        r"(data engineer|etl developer|dwh developer|data pipeline|hadoop|spark|"
        r"big[ -]?data|инженер данных|data platform engineer|data ops|data architect|"
        r"data integration|pipeline engineer|data architect|data warehouse|etl|data lakes|"
        r"database engineer|etl integration|apache airflow|data migration)",
        title_lower,
    ):
        return "Data Engineering"

    elif re.search(
        r"(bi[ -]?аналитик|power bi|tableau|qlik|looker|reporting|business intelligence|"
        r"bi developer|bi consultant|bi engineer|dashboards|data visualization|dashboard developer|"
        r"отчётность|аналитик данных|reporting engineer|sql developer|business analysis)",
        title_lower,
    ):
        return "BI"

    elif re.search(
        r"(аналитик|business analyst|data analyst|финансов\w* аналитик|performance analyst|"
        r"market analyst|web[ -]?аналитик|маркетингов\w* аналитик|product analyst|"
        r"analytics engineer|kpi аналитик|market research analyst|производственн\w* аналитик|"
        r"исследователь данных|системн\w* аналитик|сегмент\w* аналитик)",
        title_lower,
    ):
        return "Analytics"

    elif re.search(
        r"(разработчик|developer|программист|engineer|инженер|frontend|backend|fullstack|"
        r"python|java|c#|\.net|golang|php|javascript|js|react|vue|angular|"
        r"embedded engineer|firmware|cloud engineer|site reliability engineer|sre|network engineer|"
        r"ios|android|devops|sql|dba|архитектор|"
        r"сопровожден|поддержк|software engineer|full stack|backend engineer|frontend engineer)",
        title_lower,
    ):
        return "DEV"

    elif re.search(
        r"(тестировщик|qa|quality assurance|automation engineer|manual qa|"
        r"qa engineer|qa analyst|software tester|test automation|functional tester)",
        title_lower,
    ):
        return "QA / Testing"

    elif re.search(
        r"(project manager|pm|product manager|руководител|team lead|scrum master|delivery manager|"
        r"руководитель проектов|менеджер проекта|продуктов\w* менеджер|product owner|po|тимлид)",
        title_lower,
    ):
        return "Management / PM"

    else:
        return "Other"


def extract_position_level(title):
    title = title.lower().strip()

    if re.search(
        r"\b(директор|руководитель|начальник|заместитель|зам|head of|"
        r"cpo|cto|cmo|cfo|ceo|"
        r"управляющий|менеджер|manager|"
        r"лидер|lead|"
        r"руковод|управление|"
        r"главный (специалист|аналитик)|"
        r"старший менеджер|team lead|"
        r"руководство|заведующий|"
        r"начальник отдела|руководитель (отдела|направления|группы))\b",
        title,
    ):
        return "Руководитель"

    elif re.search(
        r"\b(senior|ведущий|главный|старший|chief|"
        r"опытный|"
        r"ведущ|"
        r"основной|"
        r"head|"
        r"ведущий специалист)\b",
        title,
    ):
        return "Сеньор"

    elif re.search(
        r"\b(младший|junior|джун|"
        r"ассистент|assistant|"
        r"помощник|"
        r"начинающий|"
        r"ученик)\b",
        title,
    ):
        return "Джун"

    elif re.search(r"\b(стажер|intern|trainee)\b", title):
        return "Стажер"

    elif re.search(r"\bmiddle\b", title) or not (
        re.search(
            r"\b(senior|ведущий|главный|старший|chief|руководитель|начальник|директор|младший|junior|джун|ассистент|стажер|intern|trainee)\b",
            title,
        )
    ):
        return "Мидл"

    else:
        return "Other"


def filter_skills(skill_list):
    if not isinstance(skill_list, list):
        return []
    return [skill for skill in skill_list if skill in key_skills]


def extract_salary(row):
    """
    Функция извлекает зарплату из строк датафрейма и вычитает налоги, а также определяет валюту
    """
    try:
        if pd.isna(row["salary"]):
            return None, None, None

        numbers = re.findall(r"\d{1,3}(?: \d{3})*", row["salary"])

        if len(numbers) == 0:
            numbers = re.findall(r"(\d+)", row["salary"])

        coef = 0.87 ** int(not row["tax_accounting"])

        min_salary = int(numbers[0].replace(" ", "")) * coef
        max_salary = int(numbers[-1].replace(" ", "")) * coef

        if len(numbers) == 1:
            if "от" in row["salary"]:
                max_salary = None
            elif "до" in row["salary"]:
                min_salary = None

        currency = "Рубли"
        if "€" in row["salary"]:
            currency = "Евро"
        elif "$" in row["salary"]:
            currency = "Доллары"

        return min_salary, max_salary, currency

    except Exception as e:
        return "repr(e)", "repr(e)", "repr(e)"


def extract_date(row):
    if row["date_published"] is None:
        return None
    month_map = {
        "января": 1,
        "февраля": 2,
        "марта": 3,
        "апреля": 4,
        "мая": 5,
        "июня": 6,
        "июля": 7,
        "августа": 8,
        "сентября": 9,
        "октября": 10,
        "ноября": 11,
        "декабря": 12,
    }
    date_str = (row["date_published"], "%d %B %Y")[0]
    day, month, year = date_str.split()
    month = month_map[month]
    date_obj = datetime(int(year), month, int(day))
    return date_obj


def get_page_content(vacancies):
    """
    Функция собирает названия вакансий, компаний и ссылок к ним с поисковых страниц
    """
    name_lst = []
    employer_lst = []
    href_lst = []

    for i in range(len(vacancies)):
        try:
            name_lst.append(
                vacancies[i].find("span", {"data-qa": "serp-item__title-text"}).text
            )
        except Exception as e:
            name_lst.append(None)

        try:
            employer_lst.append(
                vacancies[i]
                .find("span", {"data-qa": "vacancy-serp__vacancy-employer-text"})
                .text.replace("\xa0", " ")
            )
        except Exception as e:
            employer_lst.append(None)
        try:
            href_lst.append(vacancies[i].a.get("href"))
        except Exception as e:
            href_lst.append(None)

    d = {
        "name": name_lst,
        "employer": employer_lst,
        "link": href_lst,
    }
    df = pd.DataFrame(data=d).reset_index(drop=True)
    return df


@dag(default_args=default_args, schedule=schedule_interval, catchup=False)
def vacancies_scraper_hh():
    """
    Основная функция DAG'а
    """

    @task
    def scraping_data():
        """
        Функция парсит все необходимые данные с hh.ru посредством вебдрайвера из библиотеки selenium.
        """
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")  # фоновый режим
        options.add_argument("window-size=1920x1080")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--incognito")

        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()), options=options
        )
        print("driver created")
        sleep(1)

        try:
            driver.delete_all_cookies()
            driver.refresh()
            driver.get("https://hh.ru/?hhtmFrom=vacancy_search_list")
            sleep(2)

            search_box = WebDriverWait(driver, 18).until(
                EC.presence_of_element_located((By.ID, "a11y-search-input"))
            )
            sleep(2)

            search_box.send_keys("аналитик данных")
            search_box.send_keys(Keys.RETURN)
            ActionChains(driver).send_keys(Keys.ESCAPE).perform()
            sleep(2)

            content = BeautifulSoup(driver.page_source, "html.parser")
            vacancies = content.find_all(
                class_="vacancy-card--n77Dj8TY8VIUF0yM font-inter"
            )

            df = get_page_content(vacancies).assign(number_page=1)
            print(f"1st search page loaded at {datetime.now().strftime('%H:%M:%S')}")

            for page in range(2, 41):
                try:
                    next_page = WebDriverWait(driver, 1).until(
                        EC.element_to_be_clickable((By.LINK_TEXT, str(page)))
                    )
                    driver.execute_script("arguments[0].click();", next_page)
                    sleep(2)

                    content = BeautifulSoup(driver.page_source, "html.parser")
                    vacancies = content.find_all(
                        class_="vacancy-card--n77Dj8TY8VIUF0yM font-inter"
                    )

                    page_content = get_page_content(vacancies).assign(number_page=page)

                    df = pd.concat([df, page_content]).reset_index(drop=True)

                    print(
                        f"{page} search page loaded at {datetime.now().strftime('%H:%M:%S')}"
                    )
                except Exception as e:
                    print(f"Page {page}: {repr(e)}")
                    break

            df = df.drop_duplicates("link").reset_index(drop=True)

            salary_lst = []
            rate_lst = []
            estimate_lst = []
            experience_lst = []
            employment_lst = []
            work_schedule_lst = []
            work_hours_lst = []
            work_format_lst = []
            links_lst = []
            skills_lst = []
            metro_lst = []
            date_published_lst = []

            for i, vacancy_url in enumerate(df.link):
                skills_set = set()
                print(f"scraping {i} link")
                try:
                    driver.get(df.link[i])
                    sleep(2)

                    page_source = driver.page_source
                    soup = BeautifulSoup(page_source, "html.parser")

                    try:
                        salary_lst.append(
                            soup.find(
                                "span",
                                {
                                    "data-qa": [
                                        "vacancy-salary-compensation-type-net",
                                        "vacancy-salary-compensation-type-gross",
                                    ]
                                },
                            )
                            .text.replace("\u202f", " ")
                            .replace("\xa0", " ")
                        )
                    except Exception as e:
                        salary_lst.append(None)

                    try:
                        if soup.find(class_="vacancy-description"):
                            vacancy_block = soup.find(class_="vacancy-description")
                        else:
                            vacancy_block = soup.find(
                                "div", {"data-qa": "vacancy-description"}
                            )

                        vacancy_text = [i.text for i in vacancy_block.find_all("li")]

                        pattern = re.compile(
                            r"\b(" + "|".join(map(re.escape, key_skills)) + r")\b",
                            flags=re.IGNORECASE,
                        )

                        for text in vacancy_text:
                            matches = pattern.findall(text)
                            for match in matches:
                                normalized_match = next(
                                    (
                                        kw
                                        for kw in key_skills
                                        if kw.lower() == match.lower()
                                    ),
                                    match,
                                )
                                skills_set.add(normalized_match)

                        if skills_set == set():
                            skills_lst.append(None)
                        else:
                            try:
                                skills_set.update(
                                    [
                                        el.text
                                        for el in soup.find_all(
                                            "li", {"data-qa": "skills-element"}
                                        )
                                    ]
                                )
                                skills_lst.append(sorted(skills_set))
                            except Exception as e:
                                skills_lst.append(sorted(skills_set))
                    except Exception as e:
                        skills_lst.append(None)
                    (
                        links_lst.append(vacancy_url)
                        if "adsrv.hh.ru" not in vacancy_url
                        else links_lst.append(driver.current_url)
                    )

                    try:
                        rate_lst.append(
                            float(
                                soup.find(
                                    "div",
                                    {
                                        "data-qa": "employer-review-small-widget-total-rating"
                                    },
                                ).text.replace(",", ".")
                            )
                        )
                    except Exception as e:
                        rate_lst.append(None)
                    try:
                        estimate_lst.append(
                            soup.find(
                                "div",
                                {
                                    "data-qa": "employer-review-small-widget-review-count-action"
                                },
                            ).text.split()[0]
                        )
                    except Exception as e:
                        estimate_lst.append(None)
                    try:
                        experience_lst.append(
                            soup.find(
                                "p", {"data-qa": "work-experience-text"}
                            ).span.text
                        )
                    except Exception as e:
                        experience_lst.append(None)
                    try:
                        employment_lst.append(
                            soup.find(
                                "div", {"data-qa": "common-employment-text"}
                            ).span.text
                        )
                    except Exception as e:
                        employment_lst.append(None)
                    try:
                        work_schedule_lst.append(
                            soup.find("p", {"data-qa": "work-schedule-by-days-text"})
                            .text.replace("График: ", "")
                            .replace(", ", " или ")
                            .split(" или ")
                        )
                    except Exception as e:
                        work_schedule_lst.append(None)
                    try:
                        work_hours_lst.append(
                            int(
                                soup.find(
                                    "div", {"data-qa": "working-hours-text"}
                                ).text.split()[-1]
                            )
                        )
                    except Exception as e:
                        work_hours_lst.append(None)
                    try:
                        work_format_lst.append(
                            soup.find("p", {"data-qa": "work-formats-text"})
                            .text.replace("Формат работы: ", "")
                            .replace(", ", " или ")
                            .split(" или ")
                        )
                    except Exception as e:
                        work_format_lst.append(None)
                    try:
                        metro_stations = [
                            metro.text
                            for metro in soup.find(
                                "div", {"data-qa": "vacancy-address-with-map"}
                            ).find_all(
                                "span", {"data-qa": "address-metro-station-name"}
                            )
                        ]
                        if metro_stations == []:
                            metro_lst.append(None)
                        else:
                            metro_lst.append(metro_stations)
                    except Exception as e:
                        metro_lst.append(None)
                    try:
                        date_published_lst.append(
                            soup.find(
                                "p", {"class": "vacancy-creation-time-redesigned"}
                            ).span.text.replace("\xa0", " ")
                        )
                    except Exception as e:
                        date_published_lst.append(None)

                except Exception as e:
                    print(f"Scraping problem with {vacancy_url}: {repr(e)}")

            d_values = {
                "salary": salary_lst,
                "link": links_lst,
                "rate": rate_lst,
                "estimate": estimate_lst,
                "experience": experience_lst,
                "employment": employment_lst,
                "work_schedule": work_schedule_lst,
                "work_hours": work_hours_lst,
                "work_format": work_format_lst,
                "skills": skills_lst,
                "metro": metro_lst,
                "date_published": date_published_lst,
            }
            df_details = pd.DataFrame(data=d_values).reset_index(drop=True)

            df = df.merge(df_details, on="link").assign(
                scraping_date=pd.to_datetime(datetime.now())
                .normalize()
                .strftime("%Y-%m-%d")
            )
            df = df.astype(object).where(pd.notnull(df), None)

            df.drop_duplicates(subset=["link"])
            return df.to_dict()
        finally:
            print("Scraping completed")
            driver.quit()

    @task
    def preprocessing_data(records):
        print("Start preprocessing")

        valute = (requests.get("https://www.cbr-xml-daily.ru/daily_json.js").json())[
            "Valute"
        ]
        usd = valute["USD"]["Value"]
        eur = valute["EUR"]["Value"]

        df = pd.DataFrame(records)
        df = df.reset_index(drop=True).rename(columns={"name": "title"})

        df["id"] = df["link"].str.extract(r"/vacancy/(\d+)[/?]?")

        df["area"] = df["title"].apply(categorize_vacancy)

        df["tax_accounting"] = ~df["salary"].str.contains("до вычета налогов", na=False)

        df[["min_salary", "max_salary", "currency"]] = df.apply(
            extract_salary, axis=1, result_type="expand"
        )
        df.loc[df["currency"] == "Евро", ["min_salary", "max_salary"]] *= eur
        df.loc[df["currency"] == "Доллары", ["min_salary", "max_salary"]] *= usd

        df["salary"] = df[["min_salary", "max_salary"]].mean(axis=1, skipna=True)

        for column in ["salary", "min_salary", "max_salary"]:
            df[column] = (df[column] / 1000).round() * 1000

        df["position_level"] = df["title"].apply(extract_position_level)

        df["date_published"] = df.apply(extract_date, axis=1)

        df["skills_filtered"] = df["skills"].apply(filter_skills)

        df_expanded = df[
            ["id", "skills_filtered", "work_format", "work_schedule", "metro"]
        ].copy()

        df_expanded = (
            df_expanded.explode("skills_filtered")
            .explode("work_format")
            .explode("metro")
            .explode("work_schedule")
            .rename(
                columns={
                    "skills_filtered": "skills_expanded",
                    "work_format": "wf_expanded",
                    "metro": "metro_expanded",
                    "work_schedule": "ws_expanded",
                }
            )
        )

        df = df[
            [
                "id",
                "title",
                "area",
                "position_level",
                "salary",
                "min_salary",
                "max_salary",
                "employer",
                "rate",
                "estimate",
                "experience",
                "skills",
                "employment",
                "work_format",
                "metro",
                "work_hours",
                "work_schedule",
                "link",
                "date_published",
                "scraping_date",
            ]
        ]

        numeric_cols = [
            "id",
            "estimate",
            "work_hours",
            "salary",
            "min_salary",
            "max_salary",
            "rate",
        ]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").round().astype("float64")

        string_cols = [
            "title",
            "area",
            "position_level",
            "employer",
            "experience",
            "employment",
            "link",
            "date_published",
            "scraping_date",
        ]
        for col in string_cols:
            df[col] = df[col].astype("string")

        json_cols = ["skills", "work_format", "metro", "work_schedule"]
        for col in json_cols:
            df[col] = df[col].apply(
                lambda x: json.dumps(x, ensure_ascii=False) if x is not None else None
            )

        df = df.astype(
            {
                "skills": "object",
                "work_format": "object",
                "metro": "object",
                "work_schedule": "object",
            }
        )

        df = df.replace({np.nan: None, pd.NA: None, pd.NaT: None})
        df_expanded = df_expanded.replace({np.nan: None, pd.NA: None, pd.NaT: None})

        return {
            "df": df.to_dict(orient="split"),
            "df_expanded": df_expanded.to_dict(orient="split"),
        }

    @task
    def load_data(df_dict):
        df = pd.DataFrame(**df_dict["df"])
        df_extra = pd.DataFrame(**df_dict["df_expanded"])

        df = df.replace([np.nan, pd.NA, "NaN", "nan"], None)
        df_extra = df_extra.replace([np.nan, pd.NA, "NaN", "nan"], None)

        def connect_to_db():
            print("Connecting to the PostgreSQL database...")
            try:
                conn = psycopg2.connect(
                    host="db",
                    port="5432",
                    dbname="db",
                    user="db_user",
                    password="db_password",
                )
                return conn
            except psycopg2.Error as e:
                print(f"Database connection failed: {e}")
                raise

        def create_table(conn):
            print("Creating table if not exist...")
            try:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    CREATE SCHEMA IF NOT EXISTS dev_v1;

                    CREATE TABLE IF NOT EXISTS dev_v1.scraper_hh (
                        id BIGINT PRIMARY KEY,
                        title TEXT,
                        area TEXT,
                        position_level TEXT,
                        salary NUMERIC,
                        min_salary NUMERIC,
                        max_salary NUMERIC,
                        employer TEXT,
                        rate NUMERIC,
                        estimate NUMERIC,
                        experience TEXT,
                        skills JSONB,
                        employment TEXT,
                        work_format JSONB,
                        metro JSONB,
                        work_hours NUMERIC,
                        work_schedule JSONB,
                        link TEXT,
                        date_published TIMESTAMP,
                        scraping_date TIMESTAMP
                    );


                    CREATE TABLE IF NOT EXISTS dev_v1.scraper_hh_extra (
                        id BIGINT PRIMARY KEY,
                        skills_expanded TEXT,
                        wf_expanded TEXT,
                        metro_expanded TEXT,
                        ws_expanded TEXT,
                        CONSTRAINT fk_scraper_hh FOREIGN KEY (id) REFERENCES dev_v1.scraper_hh(id) ON DELETE CASCADE
                    );
                    """
                )
                conn.commit()
                print("Tables were created.")
            except psycopg2.Error as e:
                print(f"Failed to create tables: {e}")
                raise

        def insert_records(conn, df, df_extra):
            print("Inserting data into the database...")
            try:
                cursor = conn.cursor()
                for _, row in df.iterrows():
                    cursor.execute(
                        """
                        INSERT INTO dev_v1.scraper_hh (
                            id,
                            title,
                            area,
                            position_level,
                            salary,
                            min_salary,
                            max_salary,
                            employer,
                            rate,
                            estimate,
                            experience,
                            skills,
                            employment,
                            work_format,
                            metro,
                            work_hours,
                            work_schedule,
                            link,
                            date_published,
                            scraping_date
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO UPDATE SET
                            title = EXCLUDED.title,
                            area = EXCLUDED.area,
                            position_level = EXCLUDED.position_level,
                            salary = EXCLUDED.salary,
                            min_salary = EXCLUDED.min_salary,
                            max_salary = EXCLUDED.max_salary,
                            employer = EXCLUDED.employer,
                            rate = EXCLUDED.rate,
                            estimate = EXCLUDED.estimate,
                            experience = EXCLUDED.experience,
                            skills = EXCLUDED.skills,
                            employment = EXCLUDED.employment,
                            work_format = EXCLUDED.work_format,
                            metro = EXCLUDED.metro,
                            work_hours = EXCLUDED.work_hours,
                            work_schedule = EXCLUDED.work_schedule,
                            link = EXCLUDED.link,
                            date_published = EXCLUDED.date_published,
                            scraping_date = EXCLUDED.scraping_date
                        """,
                        (
                            row["id"],
                            row["title"],
                            row["area"],
                            row["position_level"],
                            row["salary"],
                            row["min_salary"],
                            row["max_salary"],
                            row["employer"],
                            row["rate"],
                            row["estimate"],
                            row["experience"],
                            Json(row["skills"]),
                            row["employment"],
                            Json(row["work_format"]),
                            Json(row["metro"]),
                            row["work_hours"],
                            Json(row["work_schedule"]),
                            row["link"],
                            row["date_published"],
                            row["scraping_date"],
                        ),
                    )

                for _, row in df_extra.iterrows():
                    cursor.execute(
                        """
                        INSERT INTO dev_v1.scraper_hh_extra (
                            id,
                            skills_expanded,
                            wf_expanded,
                            metro_expanded,
                            ws_expanded
                        ) VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO UPDATE SET
                            skills_expanded = EXCLUDED.skills_expanded,
                            wf_expanded = EXCLUDED.wf_expanded,
                            metro_expanded = EXCLUDED.metro_expanded,
                            ws_expanded = EXCLUDED.ws_expanded
                        """,
                        (
                            row["id"],
                            row["skills_expanded"],
                            row["wf_expanded"],
                            row["metro_expanded"],
                            row["ws_expanded"],
                        ),
                    )

                conn.commit()
                print("Data successfully inserted")
            except psycopg2.Error as e:
                print(f"Error inserting data into database: {e}")
                conn.rollback()
                raise
            except Exception as e:
                print(f"Unexpected error: {e}")
                conn.rollback()
                raise

        try:
            conn = connect_to_db()
            create_table(conn)
            insert_records(conn, df, df_extra)
        except Exception as e:
            print(f"An error occurred during execution: {e}")
        finally:
            if "conn" in locals():
                conn.close()
                print("Database connection closed.")

    df = scraping_data()
    df_dict = preprocessing_data(df)
    load_data(df_dict)

    return


dag = vacancies_scraper_hh()
