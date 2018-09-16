#!/usr/bin/env python3

import pymysql.cursors

connection = pymysql.connect(host='localhost',
                             user='root',
                             password='',
                             db='logs',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

try:
    print("1. Посетители из какой страны совершают больше всего действий на сайте?")
    with connection.cursor() as cursor:
        sql = "SELECT `user`.`country` , count(*) as `count_actions` \
               FROM `user`, `action` \
               WHERE `user`.`id`=`action`.`user` \
               AND `user`.`country` IN ('China', 'Germany', 'United States', 'Japan', 'United Kingdom') \
               GROUP BY `user`.`country` \
               ORDER BY `count_actions` DESC"
        cursor.execute(sql, ())
        result = cursor.fetchall()
        for value in result :
            print(value['country'])
        print()

    print("2. Посетители из какой страны чаще всего интересуются товарами из категории “fresh_fish”?")
    with connection.cursor() as cursor:
        sql = "SELECT `user`.`country` , count(*) as `count_actions` \
               FROM `user`, `action` \
               WHERE `user`.`id`=`action`.`user` \
               AND `user`.`country` IN ('China', 'Germany', 'United States', 'Japan', 'United Kingdom') \
               AND `action`.`product_category`=(SELECT `id` FROM `product_category` WHERE `name`='fresh_fish') \
               GROUP BY `user`.`country` \
               ORDER BY `count_actions` DESC"
        cursor.execute(sql, ())
        result = cursor.fetchall()
        for value in result :
            print(value['country'])
        print()

    print("3. В какое время суток чаще всего просматривают категорию “frozen_fish”?")
    with connection.cursor() as cursor:
        sql = "SELECT `times`.`time` \
               FROM( \
               (SELECT count(*) as `count`, 'night (00:00 - 06:00)' as `time` \
               FROM `action` \
               WHERE time(`datetime`) BETWEEN '00:00:00' AND '05:59:59' \
               AND `product_category`=(SELECT `id` FROM `product_category` WHERE `name`='frozen_fish') \
               )UNION( \
               SELECT count(*) as `count`, 'morning (06:00 - 12:00)' as `time` \
               FROM `action` \
               WHERE time(`datetime`) BETWEEN '06:00:00' AND '11:59:59' \
               AND `product_category`=(SELECT `id` FROM `product_category` WHERE `name`='frozen_fish') \
               )UNION( \
               SELECT count(*) as `count`, 'day (12:00 - 18:00)' as `time` \
               FROM `action` \
               WHERE time(`datetime`) BETWEEN '12:00:00' AND '17:59:59' \
               AND `product_category`=(SELECT `id` FROM `product_category` WHERE `name`='frozen_fish') \
               )UNION( \
               SELECT count(*) as `count`, 'evening (18:00 - 0:00)' as `time` \
               FROM `action` \
               WHERE time(`datetime`) BETWEEN '18:00:00' AND '23:59:59' \
               AND `product_category`=(SELECT `id` FROM `product_category` WHERE `name`='frozen_fish') \
               )) as `times` \
               ORDER BY `times`.`count` DESC"
        cursor.execute(sql, ())
        result = cursor.fetchone()
        print(result['time'])
        print()

    print("4. Какое максимальное число запросов на сайт за астрономический час (c 00 минут 00 секунд до 59 минут 59 секунд)?")
    with connection.cursor() as cursor:
        sql = "SELECT count(*) as count \
               FROM `action` \
               GROUP BY addtime(DATE_FORMAT(date(`datetime`), '%%Y-%%m-%%d %%H:%%i'), CONCAT(hour(`datetime`), ':', '00')) \
               ORDER BY count DESC"
        cursor.execute(sql, ())
        result = cursor.fetchone()
        print(result['count'])
        print()

    print("5. Товары из какой категории чаще всего покупают совместно с товаром из категории “semi_manufactures”?")
    with connection.cursor() as cursor:
        sql = '''SELECT product_category.name, count(*) as count
                 FROM cart_to_user, product_category, action
                 WHERE cart_to_user.action = action.id
                 AND action.product_category = product_category.id
                 AND cart IN (
                     SELECT cart_to_user.cart
                     FROM cart_to_user, action
                     WHERE cart_to_user.action = action.id
                     AND action.product_category=(SELECT id FROM product_category WHERE name='semi_manufactures')
                     )
                 GROUP BY action.product_category
                 HAVING product_category.name <> 'semi_manufactures'
                 ORDER BY count DESC'''
        cursor.execute(sql, ())
        result = cursor.fetchone()
        print(result['name'])
        print()

    print("6. Сколько брошенных (не оплаченных) корзин имеется?")
    with connection.cursor() as cursor:
        sql = '''SELECT count(*) as count
                 FROM cart
                 GROUP BY success_pay_flag
                 HAVING success_pay_flag=0'''
        cursor.execute(sql, ())
        result = cursor.fetchone()
        print(result['count'])
        print()

    print("7. Какое количество пользователей совершали повторные покупки?")
    with connection.cursor() as cursor:
        sql = '''SELECT count(*) as count
                 FROM
                 (
                     SELECT tt.user, count(*) as count
                     FROM
                     (
                         SELECT user, cart
                         FROM cart_to_user
                         WHERE cart IN (SELECT id FROM cart WHERE success_pay_flag=1)
                         GROUP BY cart
                     ) as tt
                     GROUP BY tt.user  
                     ORDER BY `count`  DESC
                 ) as dd
                 WHERE dd.count>1'''
        cursor.execute(sql, ())
        result = cursor.fetchone()
        print(result['count'])
        print()
    

finally:
    connection.close()
