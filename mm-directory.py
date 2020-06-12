import requests
from bs4 import BeautifulSoup
import sqlite3


def prepare_table(table_name):
    conn = sqlite3.connect('mm_directory.db')
    c = conn.cursor()
    status = False

    # get the count of tables with the name
    sql = ''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name=? '''
    c.execute(sql, (table_name,))

    # if the count is 1, then table exists
    if c.fetchone()[0] == 1:
        sql = 'DROP TABLE {0}'.format(table_name)
        c.execute(sql)

    # Save (commit) the changes
    conn.commit()
    # We can also close the connection if we are done with it.
    # Just be sure any changes have been committed or they will be lost.
    conn.close()


def load_one_page_data(url, cat_id):
    conn = sqlite3.connect('mm_directory.db')
    table_info_detail = 'info_detail'
    c = conn.cursor()
    print("Loading......")
    single_page = requests.get(url)
    single_soup = BeautifulSoup(single_page.content, "html.parser")
    single_body = single_soup.body.section.div.div.div.contents
    info_rows = single_body[9]
    sql = "INSERT INTO " + table_info_detail + "(name,address,city,ph_no,cat_id) VALUES (?,?,?,?,?)"
    for row in info_rows.find_all('div', "row detail_row"):
        info_block = row.div.div.div.div.contents
        title_p = info_block[1]
        address_p = info_block[3]
        city_p = info_block[5]
        phone_number_p = info_block[7]
        if title_p.sup:
            title_p.sup.decompose()
        category_span = title_p.span
        category = category_span.get_text().strip()
        category_span.decompose()
        title = title_p.get_text().strip()
        address = address_p.get_text().strip()
        city = city_p.get_text().strip()
        phone_numbers = phone_number_p.get_text().strip()
        c.execute(sql, (title, address, city, phone_numbers, cat_id))
        print(title)
        print(category)
        print(address)
        print(city)
        print(phone_numbers)

    # Save (commit) the changes
    conn.commit()

    print("Complete info Detail Download")

    # We can also close the connection if we are done with it.
    # Just be sure any changes have been committed or they will be lost.
    conn.close()


def load_data(city, cat_id):
    base_url = 'https://www.myanmarbusiness-directory.com'
    full_url = '{0}/en/categories-index/search.html?city={1}&cat_id={2}'.format(base_url, city, cat_id)
    page = requests.get(full_url)
    soup = BeautifulSoup(page.content, "html.parser")
    body = soup.body.section.div.div.div.contents
    pagination_rows = body[9].find('div', 'pagination').ul
    load_one_page_data(full_url, cat_id)
    if pagination_rows:
        for pagination_row in pagination_rows.find_all('a'):
            if pagination_row.get_text().strip().isnumeric():
                print(pagination_row['href'])
                load_one_page_data('{0}{1}'.format(base_url, pagination_row['href']), cat_id)


def get_category_of_city(city):
    conn = sqlite3.connect('mm_directory.db')
    table_categories_name = 'categories'
    prepare_table(table_categories_name)
    c = conn.cursor()

    # Create table
    operation = ''' CREATE TABLE ''' + table_categories_name + '''
                     (cat_id number, cat_name text)'''
    c.executescript(operation)
    print("Loading......")
    category_list = []
    categories_of_city_url = 'https://www.myanmarbusiness-directory.com/modules/mod_mt_advsearch/getCity.php?val=' + city
    categories_page = requests.get(categories_of_city_url)
    categories_soup = BeautifulSoup(categories_page.content, "html.parser")
    sql = 'INSERT INTO ' + table_categories_name + '(cat_id,cat_name) VALUES (?,?)'
    for category_tag in categories_soup.find_all('option'):
        category_value = category_tag['value']
        category_text = category_tag.get_text()
        c.execute(sql, (category_value, category_text))
        category_list.append(category_value)

    # Save (commit) the changes
    conn.commit()

    print("Complete Category List Download")

    # We can also close the connection if we are done with it.
    # Just be sure any changes have been committed or they will be lost.
    conn.close()
    return category_list


def get_city_list():
    conn = sqlite3.connect('mm_directory.db')
    table_city_name = 'cities'
    prepare_table(table_city_name)
    c = conn.cursor()

    # Create table
    c.execute('''CREATE TABLE ''' + table_city_name + '''
                 (city_id number, city_name text)''')
    city_list_url = 'https://www.myanmarbusiness-directory.com/en/'
    print("Loading......")
    city_list_page = requests.get(city_list_url)
    city_list_soup = BeautifulSoup(city_list_page.content, "html.parser")
    count = 1
    sql = "INSERT INTO " + table_city_name + "(city_id,city_name) VALUES (?,?)"
    for city_tag in city_list_soup.find("select", {"id": "mt_search_township"}).find_all('option'):
        city = city_tag['value']
        # Insert a row of data
        c.execute(sql, (count, city))
        count += 1

    # Save (commit) the changes
    conn.commit()

    print("Complete City List Download")

    # We can also close the connection if we are done with it.
    # Just be sure any changes have been committed or they will be lost.
    conn.close()


def main():
    status = True
    while status:
        print('''
            Welcome 
            Please choose number:
            1 : 'Get City List'
            2 : 'Get Category List of City'
            3 : 'Get Info Detail One Category'
            4 : 'Get All Info Detail Category List of City'
        ''')
        key_input = input()
        if key_input == '1':
            get_city_list()
        elif key_input == '2':
            city_input = input("Enter a city name")
            print(get_category_of_city(city_input))
        elif key_input == '3':
            conn = sqlite3.connect('mm_directory.db')
            table_info_detail = 'info_detail'
            prepare_table(table_info_detail)
            c = conn.cursor()

            # Create table
            operation = ''' CREATE TABLE ''' + table_info_detail + '''
                                                (name text, address text, city text, ph_no text, cat_id number)'''
            c.executescript(operation)
            conn.commit()
            conn.close()
            city_input = input("Enter a city name")
            category_id = input("Enter a category id")
            load_data(city_input, category_id)
        elif key_input == '4':
            conn = sqlite3.connect('mm_directory.db')
            table_info_detail = 'info_detail'
            prepare_table(table_info_detail)
            c = conn.cursor()

            # Create table
            operation = ''' CREATE TABLE ''' + table_info_detail + '''
                                     (name text, address text, city text, ph_no text, cat_id number)'''
            c.executescript(operation)
            conn.commit()
            conn.close()
            city_input = input("Enter a city name")
            for category_id in get_category_of_city(city_input):
                load_data(city_input, category_id)
        ans = input("Are you continue? (yes,no)")
        if ans.lower() == 'no':
            status = False
        else:
            status = True


if __name__ == '__main__':
    main()
