# pylint:disable=C0111,C0103
import sqlite3

conn = sqlite3.connect('data/ecommerce.sqlite')
dbb = conn.cursor()


def order_rank_per_customer(db):
    query = '''
    SELECT orders.OrderID, orders.CustomerID, orders.OrderDate,
    RANK() OVER (
            PARTITION BY orders.CustomerID
            ORDER BY orders.OrderDate
        ) AS order_rank
    FROM Orders
    '''
    db.execute(query)
    results = db.fetchall()
    return results


def order_cumulative_amount_per_customer(db):

    query = '''
    SELECT orders.OrderID, orders.CustomerID, orders.OrderDate,
    SUM(SUM(orderdetails.Quantity * orderdetails.UnitPrice)) OVER (
            PARTITION BY orders.CustomerID
            ORDER BY orders.OrderDate
        ) AS cumulative_amount
    FROM  Orders
    JOIN  OrderDetails
        ON  orders.OrderID = orderdetails.OrderID
    GROUP BY orders.OrderID
    ORDER BY orders.CustomerID
    '''
    db.execute(query)
    results = db.fetchall()
    return results



#print(order_rank_per_customer(db))
#print(order_cumulative_amount_per_customer(db))
