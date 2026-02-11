create schema if not exists portfolio;

create table if not exists portfolio.person (
  id bigint primary key,
  full_name varchar(100) not null,
  headline varchar(200) not null,
  location varchar(100) not null,
  employment_type varchar(50) not null,
  years_experience int not null,
  summary text not null
);

create table if not exists portfolio.skills (
  id bigserial primary key,
  person_id bigint not null references portfolio.person(id),
  name varchar(80) not null,
  level int not null check (level between 1 and 5),
  years int not null check (years >= 0),
  last_used date
);

create table if not exists portfolio.projects (
  id bigserial primary key,
  person_id bigint not null references portfolio.person(id),
  title varchar(160) not null,
  role varchar(100) not null,
  domain varchar(100),
  start_date date not null,
  end_date date,
  outcome text,
  tech_stack text
);

create index if not exists idx_skills_person on portfolio.skills(person_id);
create index if not exists idx_skills_name on portfolio.skills(name);
create index if not exists idx_projects_person on portfolio.projects(person_id);
create index if not exists idx_projects_domain on portfolio.projects(domain);

insert into portfolio.person(id, full_name, headline, location, employment_type, years_experience, summary) values
(1, 'Hack Pereira', 'Desenvolvedor Full-Stack | Java e SQL', 'São Paulo, BR', 'Autônomo', 7, 'Profissional autônomo que constrói lojas e soluções de varejo ponta a ponta, com foco em integrações, dados e performance. Forte em Java, modelagem relacional, ETL e arquitetura orientada a domínio.');

insert into portfolio.skills(person_id, name, level, years, last_used) values
(1, 'Java', 5, 7, current_date),
(1, 'SQL', 5, 7, current_date),
(1, 'Modelagem de Dados', 5, 6, current_date),
(1, 'ETL', 4, 5, current_date),
(1, 'PostgreSQL', 4, 5, current_date),
(1, 'MySQL', 4, 4, current_date),
(1, 'SQL Server', 3, 3, current_date),
(1, 'Spring Boot', 4, 5, current_date),
(1, 'Kafka', 3, 3, current_date),
(1, 'Docker', 4, 4, current_date),
(1, 'GCP', 3, 2, current_date),
(1, 'AWS', 3, 2, current_date),
(1, 'JavaScript', 4, 5, current_date),
(1, 'React', 3, 3, current_date);

insert into portfolio.projects(person_id, title, role, domain, start_date, end_date, outcome, tech_stack) values
(1, 'Omnichannel Store V2', 'Tech Lead', 'Varejo', '2024-02-01', '2025-01-30', 'Checkout unificado, catálogo central e fidelidade integrada; +22% conversão web.', 'Java, Spring Boot, PostgreSQL, Redis, Kafka, React'),
(1, 'Modernização de POS', 'Arquiteto', 'Varejo', '2023-05-01', '2024-01-15', 'POS modular com sincronização offline; redução de filas em 17%.', 'Java, Postgres, SQLite, gRPC'),
(1, 'Data Lake de Inventário', 'Engenheiro de Dados', 'Supply Chain', '2022-09-01', '2023-04-01', 'Camadas bronze/prata/ouro com curadoria; acurácia de estoque +12%.', 'GCP, BigQuery, Airflow, Kafka, Java'),
(1, 'Busca de Catálogo', 'Full-Stack', 'E-commerce', '2021-03-01', '2022-02-01', 'Pesquisa facetada com sinônimos e relevância; CTR +15%.', 'Java, Elasticsearch, PostgreSQL, React'),
(1, 'Migração ERP', 'Engenheiro', 'Backoffice', '2020-01-01', '2020-12-01', 'Migração para microsserviços e mensageria; menos incidentes críticos.', 'Java, Spring, RabbitMQ, MySQL');

create or replace view portfolio.v_person_profile as
select p.full_name, p.headline, p.location, p.employment_type, p.years_experience from portfolio.person p;

select * from portfolio.v_person_profile;

select name, level, years, (level * 2 + years) as score
from portfolio.skills
where person_id = 1
order by score desc, last_used desc
limit 5;

with ranked as (
  select name, years, level, rank() over (order by years desc, level desc) r
  from portfolio.skills
  where person_id = 1
)
select * from ranked where r <= 5;

select title, role, domain,
       case when end_date is null
            then date_part('day', current_date - start_date)
            else date_part('day', end_date - start_date)
       end as duration_days
from portfolio.projects
where person_id = 1
order by duration_days desc;

select domain,
       count(*) as projetos,
       sum(case when end_date is null then 1 else 0 end) as em_andamento
from portfolio.projects
where person_id = 1
group by domain
order by projetos desc;

select title
from portfolio.projects
where person_id = 1
  and (tech_stack ilike '%Java%' or tech_stack ilike '%SQL%')
order by title;
