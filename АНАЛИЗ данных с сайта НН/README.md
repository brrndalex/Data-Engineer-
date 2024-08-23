# **АНАЛИЗ данных с сайта НН.**
[![Typing SVG](https://readme-typing-svg.herokuapp.com?font=Fira+Code&pause=1000&color=4EF752&width=435&lines=%D0%90%D0%9D%D0%90%D0%9B%D0%98%D0%97+;%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D1%85+;%D1%81+%D1%81%D0%B0%D0%B9%D1%82%D0%B0+%D0%9D%D0%9D.)](https://git.io/typing-svg)
# Цель,задача проекта
Провести анализ вакансии по специализации аналитика данных на основе файлов с выгрузкой данных с сайта headhunter, получив ответы на поставленные вопросы.

# Действия в рамках проекта
[Презентация проекта](https://github.com/brrndalex/Data-Engineer-Projects/blob/main/%D0%90%D0%9D%D0%90%D0%9B%D0%98%D0%97%20%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D1%85%20%D1%81%20%D1%81%D0%B0%D0%B9%D1%82%D0%B0%20%D0%9D%D0%9D/%D0%A4%D0%B8%D0%BB%D0%BE%D0%BD%D0%B5%D0%BD%D0%BA%D0%BE%20%D0%90%D0%BB%D0%B5%D0%BA%D1%81%D0%B0%D0%BD%D0%B4%D1%80%20-%20SQL_%20%D0%9F%D1%80%D0%B5%D0%B7%D0%B5%D0%BD%D1%82%D0%B0%D1%86%D0%B8%D1%8F%20%D0%BF%D0%BE%20%D0%BC%D0%BE%D0%B4%D1%83%D0%BB%D1%8C%D0%BD%D0%BE%D0%BC%D1%83%20%D0%BF%D1%80%D0%BE%D0%B5%D0%BA%D1%82%D1%83..pdf)  

1 ЭТАП: Используя библиотеку pandas, прочитал файлы excel в датафреймы(df=pd.read_excel('/путь/файл') и провел первоначальный анализ-отчет с помощью библиотеки pandas_profiling (df.profile_report()), а также очистку данных(удалил дубликаты строк данных и оставил в датафрейме по вакансиям только нужные столбцы). [Код в Google Colab.](https://github.com/brrndalex/Data-Engineer-Projects/blob/main/%D0%90%D0%9D%D0%90%D0%9B%D0%98%D0%97%20%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D1%85%20%D1%81%20%D1%81%D0%B0%D0%B9%D1%82%D0%B0%20%D0%9D%D0%9D/FILONENKO_PROJECT__SQL-1.ipynb)


2 ЭТАП: используя Python (библиотеку pandas), подключился к базе данных, создал новую базу 'vacancies_data_analysis.db', загрузил в нее 3 таблицы ('area', 'employer', 'vacancies') с данными. [Код в Google Colab.](https://github.com/brrndalex/Data-Engineer-Projects/blob/main/%D0%90%D0%9D%D0%90%D0%9B%D0%98%D0%97%20%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D1%85%20%D1%81%20%D1%81%D0%B0%D0%B9%D1%82%D0%B0%20%D0%9D%D0%9D/FILONENKO_PROJECT__SQL-1.ipynb)

3 ЭТАП: Используя инструмент draw.io, описал [физическую модель данных](https://github.com/brrndalex/Data-Engineer-Projects/blob/main/%D0%90%D0%9D%D0%90%D0%9B%D0%98%D0%97%20%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D1%85%20%D1%81%20%D1%81%D0%B0%D0%B9%D1%82%D0%B0%20%D0%9D%D0%9D/%D0%A4%D0%B8%D0%B7%D0%B8%D1%87%D0%B5%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BC%D0%BE%D0%B4%D0%B5%D0%BB%D1%8C%20%D0%BF%D0%BE%20%D0%BF%D1%80%D0%BE%D0%B5%D0%BA%D1%82%D1%83%20SQL-2.png) для 3 таблиц('area', 'employer', 'vacancies'), загруженных в базу данных (DBeaver/SQLite).

4 ЭТАП: С помощью [SQL-запросов](https://github.com/brrndalex/Data-Engineer-Projects/blob/main/%D0%90%D0%9D%D0%90%D0%9B%D0%98%D0%97%20%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D1%85%20%D1%81%20%D1%81%D0%B0%D0%B9%D1%82%D0%B0%20%D0%9D%D0%9D/SQL-%D0%B7%D0%B0%D0%BF%D1%80%D0%BE%D1%81%D1%8B.sql) в DBeaver/SQLite решил поставленные задачи (получил ответы): Определил общее кол-во вакансий, среднюю вилку зарплат,сколько вакансий предлагают работу без опыта,сколько вакансий относятся к разным значениям,средние вилки зарплат в разрезе опыта работы,количество вакансий по городам,топ - 10 рейтинг работодателей. Нашел дату последней опубликованной вакансии, вакансию с максимально предлагаемой зарплатой.

# Выводы по проекту
 Вакансия "аналитика данных" является востребованной; наиболее востребована в г. Москва, в банках и Ozon; предпочтительный режим работы: полный день или удаленная работа; зарплата от 138697 руб. до 179783 руб.
 
 # Используемые навыки и инструменты
 * Python (библиотеки: Pandas).
 * SQL.
 * SQLite.
 * Google Colab.
 * DBeaver
 * Draw.io.

# Статус
- [x] Завершен
