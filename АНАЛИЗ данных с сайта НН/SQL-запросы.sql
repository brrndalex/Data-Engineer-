



--ЗАДАЧА 1. ПОСЧИТАТЬ ОБЩЕЕ КОЛИЧЕСТВО ВАКАНСИЙ.

SELECT 
      COUNT(*) AS count_vac
FROM 
      vacancies v 


      
      
      
      
--ЗАДАЧА 2. НАЙТИ ДАТУ ПОСЛЕДНЕЙ ОПУБЛИКОВАННОЙ ВАКАНСИИ.

SELECT 
     MAX(created_at) 
FROM 
     vacancies v 
     
     
     
     
     
     
--ЗАДАЧА 3. НАЙТИ ВАКАНСИЮ С МАКСИМАЛЬНО ПРЕДЛАГАЕМОЙ ЗАРПЛАТОЙ ПО ВИЛКЕ.
     
SELECT --id,
       name, 
       MAX("salary.to" ) AS max_salary 
FROM   
       vacancies v 
   
       
       
       
       
       
 --ЗАДАЧА 4. ПОСЧИТАТЬ СРЕДНЮЮ ВИЛКУ ЗАРПЛАТ.
 -- Нижнюю границу зарплаты посчитать как среднее по всем указанным в вакансиях salary_from. 
 --Верхнюю границу вилки посчитать аналогично, только по полю salary.to.
 
SELECT  
        ROUND(AVG("salary.from")) AS avg_salary_from, 
        ROUND(AVG("salary.to")) AS avg_salary_to
FROM 
        vacancies v 
       
       
        
       
 --ЗАДАЧА 5. ПОСЧИТАТЬ СКОЛЬКО ВАКАНСИЙ ПРЕДЛАГАЕТ РАБОТУ БЕЗ ОПЫТА. 
 
SELECT "experience.name", 
        COUNT(*) 
FROM 
      vacancies v  
WHERE 
      "experience.name" ='Нет опыта'     
   
      
      
      
      
--ЗАДАЧА 6. ПОСЧИТАТЬ СКОЛЬКО ВАКАНСИЙ ОТНОСИТСЯ К РАЗНЫМ ЗНАЧЕНИЯМ schedule.name('Полный день', 'Удаленная работа', 'Гибкий график').
      
SELECT 
      "schedule.name", 
       COUNT(*) 
FROM 
       vacancies v  
WHERE 
       "schedule.name" IN ('Полный день', 'Удаленная работа', 'Гибкий график')
GROUP BY 
       "schedule.name"; 
 
      
      
      
      
--ЗАДАЧА 7. ПОСЧИТАТЬ СРЕДНИЕ ВИЛКИ ЗАРПЛАТ В РАЗРЕЗЕ ОПЫТА РАБОТЫ  experience.name.      

SELECT  
        "experience.name", 
        ROUND(AVG("salary.from")) AS avg_salary_from, 
        ROUND(AVG("salary.to")) AS avg_salary_to
FROM 
        vacancies v 
GROUP BY 
        "experience.name"; 
       
       

       
       

 --ЗАДАЧА 8. ПОСЧИТАТЬ КОЛИЧЕСТВО ВАКАНСИЙ ПО ГОРОДАМ, ВЫВЕСТИ НАЗВАНИЕ ГОРОДА ИЗ СПРАВОЧНИКА И КОЛИЧЕСТВОВАКАНСИЙ В ЭТОМ ГОРОДЕ.
   
SELECT 
       a.area_name, 
       COUNT(*) as vacancy_count
FROM 
       vacancies v  
JOIN area a  
       ON v."area.id" = a.area_id 
GROUP BY 
        a.area_name;
       
       
       
       
       
       
--ЗАДАЧА 9. СДЕЛАТЬ ТОП-10 РЕЙТИНГ РАБОТОДАТЕЛЕЙ С ИХ НАЗВАНИЯМИ ПО ЧИСЛУ ОПУБЛИКОВАННЫХ ВАКАНСИЙ.
       
SELECT 
      e.employer_name, 
      COUNT(*) AS vacancy_count
FROM 
      vacancies v 
JOIN employer e  
      ON e.employer_id = v."employer.id"
GROUP BY 
      e.employer_name
ORDER BY 
      vacancy_count DESC
LIMIT 10;       
       