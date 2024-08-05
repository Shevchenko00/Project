import mysql.connector
from connector import ConnectorRead


def query_added(query):
    dbconfig = {
        'host': 'mysql.itcareerhub.de',
        'user': 'ich1',
        'password': 'ich1_password_ilovedbs',
        'database': '310524ptm_O_Shevchenko'
    }
    connection = mysql.connector.connect(**dbconfig)
    cursor = connection.cursor()
    cursor.execute(f"SELECT * FROM query_results WHERE query = '{query}'")
    res = cursor.fetchall()

    if len(res) > 0:
        cursor.execute(f"UPDATE query_results SET count = {res[0][-1] + 1} WHERE id = {res[0][0]}")
    else:
        cursor.execute(f"INSERT INTO query_results (query, count) VALUES ('{query}', 1)")

    connection.commit()
    cursor.close()
    connection.close()


def get_result_search(search_str):
    select = f"""select distinct
 t1.film_id, t1.title, t1.description, t1.release_year, 
t3.name as name, 
 t5.first_name, t5.last_name
from film as t1
left join film_category as t2
on t1.film_id = t2.film_id
left join category as t3
on t3.category_id = t2.category_id
left join film_actor as t4
on t1.film_id = t4.actor_id
left join actor as t5
on t4.actor_id = t5.actor_id
where title like '%{search_str}%' or 
description like '%{search_str}%' or 
release_year like '%{search_str}%' or
first_name like '%{search_str}%' or
name like '%{search_str}%' limit 5000;
    """

    con = ConnectorRead.get_connect()
    con.connect()
    cursor = con.cursor()
    cursor.execute(select)
    result = cursor.fetchall()
    cursor.close()
    con.close()

    query_added(search_str)
    return result


def fetch_result(result):
    end_result = {}
    for element in result:
        if element[0] in end_result:
            end_result[element[0]]['category'].append(element[-1])
        else:
            end_result[element[0]] = {
                'title': element[1],
                'text': element[2],
                'date': element[3],
                'category': [element[4]],
                'actor': [element[-1]]
            }
    return end_result


def get_top_result():
    dbconfig = {
        'host': 'mysql.itcareerhub.de',
        'user': 'ich1',
        'password': 'ich1_password_ilovedbs',
        'database': '310524ptm_O_Shevchenko'
    }
    connection = mysql.connector.connect(**dbconfig)
    cursor = connection.cursor()
    cursor.execute(f"SELECT * FROM query_results order by count desc limit 10")
    res = cursor.fetchall()
    for i in res:
        print(f'{i[1]}\t{i[-1]}')


# Основной код
search_param = input('Your search parameter or "Top Result": ').lower()
if search_param == 'top result':
    get_top_result()
else:
    results = get_result_search(search_param)
    processed_results = fetch_result(results)

    c = 0
    for element in processed_results:
        category = ', '.join(processed_results[element]['category'])
        c += 1
        output = f"{c}: {processed_results[element]['title']}\t{processed_results[element]['text']}\t{processed_results[element]['date']}\t{category}"
        print(output)
