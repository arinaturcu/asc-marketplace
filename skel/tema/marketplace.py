"""
This module represents the Marketplace.

Computer Systems Architecture Course
Assignment 1
March 2021
"""

import unittest
from threading import Lock
from tema.product import Coffee, Tea


class Marketplace:
    """
    Class that represents the Marketplace. It's the central part of the implementation.
    The producers and consumers use its methods concurrently.
    """
    def __init__(self, queue_size_per_producer):
        """
        Constructor

        :type queue_size_per_producer: Int
        :param queue_size_per_producer: the maximum size of a queue associated with each producer
        """
        self.queue_size_per_producer = queue_size_per_producer
        self.producer_id_gen = -1
        self.carts = []
        self.queue = []
        self.print_lock = Lock()

    def register_producer(self):
        """
        Returns an id for the producer that calls this.
        """
        self.producer_id_gen += 1
        self.queue.append([])
        return self.producer_id_gen

    def publish(self, producer_id, product):
        """
        Adds the product provided by the producer to the marketplace

        :type producer_id: String
        :param producer_id: producer id

        :type product: Product
        :param product: the Product that will be published in the Marketplace

        :returns True or False. If the caller receives False, it should wait and then try again.
        """
        if producer_id > self.producer_id_gen:
            return False

        if len(self.queue[producer_id]) >= self.queue_size_per_producer:
            return False

        self.queue[producer_id].append(product)

        return True

    def new_cart(self):
        """
        Creates a new cart for the consumer

        :returns an int representing the cart_id
        """
        self.carts.append([])
        return len(self.carts) - 1

    def add_to_cart(self, cart_id, product):
        """
        Adds a product to the given cart. The method returns

        :type cart_id: Int
        :param cart_id: id cart

        :type product: Product
        :param product: the product to add to cart

        :returns True or False. If the caller receives False, it should wait and then try again
        """
        producer_id = -1
        for temp_producer_id in range(self.producer_id_gen + 1):
            if product in self.queue[temp_producer_id]:
                producer_id = temp_producer_id

        if producer_id == -1:
            return False

        self.carts[cart_id].append((product, producer_id))
        self.queue[producer_id].remove(product)

        return True

    def remove_from_cart(self, cart_id, product):
        """
        Removes a product from cart.

        :type cart_id: Int
        :param cart_id: id cart

        :type product: Product
        :param product: the product to remove from cart
        """
        producer_id = -1
        for pair in self.carts[cart_id]:
            if pair[0] == product:
                producer_id = pair[1]

        if producer_id == -1:
            return

        self.carts[cart_id].remove((product, producer_id))
        self.queue[producer_id].append(product)

    def place_order(self, cart_id):
        """
        Return a list with all the products in the cart.

        :type cart_id: Int
        :param cart_id: id cart
        """
        if len(self.carts) <= cart_id:
            return []

        cart = list(map(lambda pair: pair[0], self.carts[cart_id]))
        return cart

    def get_print_lock(self):
        return self.print_lock

class TestMarketplace(unittest.TestCase):

    def test_register_producer(self):
        marketplace = Marketplace(20)
        self.assertEqual(marketplace.register_producer(), 0)

        marketplace.producer_id_gen = 6
        self.assertEqual(marketplace.register_producer(), 7)

    def test_publish(self):
        marketplace = Marketplace(2)

        marketplace.register_producer()

        tea = Tea(name='Test', price=12, type='test type')
        self.assertTrue(marketplace.publish(0, tea))

        marketplace.publish(0, Tea(name='Test', price=12, type='test type'))

        coffee = Coffee(name='Test', price=12, acidity='test type', roast_level='MEDIUM')
        self.assertFalse(marketplace.publish(0, coffee))

    def test_new_cart(self):
        marketplace = Marketplace(3)
        self.assertEqual(marketplace.new_cart(), 0)
        self.assertEqual(marketplace.new_cart(), 1)

    def test_add_to_cart(self):
        marketplace = Marketplace(5)

        marketplace.new_cart()
        self.assertFalse(marketplace.add_to_cart(0, Tea(name='Test', price=12, type='test type')))

        marketplace.publish(0, Tea(name='Test', price=12, type='test type'))
        self.assertFalse(marketplace.add_to_cart(0, Tea(name='Test', price=12, type='test type')))

        marketplace.register_producer()
        marketplace.publish(0, Tea(name='Test', price=12, type='test type'))
        self.assertTrue(marketplace.add_to_cart(0, Tea(name='Test', price=12, type='test type')))
        self.assertEqual([(Tea(name='Test', price=12, type='test type'), 0)], marketplace.carts[0])

    def test_remove_from_cart(self):
        marketplace = Marketplace(20)

        marketplace.new_cart()
        marketplace.register_producer()
        marketplace.publish(0, Tea(name='Test', price=12, type='test type'))

        marketplace.remove_from_cart(0, Tea(name='Test', price=12, type='test type'))

        self.assertEqual([], marketplace.carts[0])

        marketplace.add_to_cart(0, Tea(name='Test', price=12, type='test type'))
        marketplace.remove_from_cart(0, Tea(name='Test', price=12, type='test type'))

        self.assertEqual([], marketplace.carts[0])

    def test_place_order(self):
        marketplace = Marketplace(5)
        self.assertEqual(marketplace.place_order(0), [])

        marketplace.new_cart()
        marketplace.register_producer()
        marketplace.publish(0, Tea(name='Test', price=12, type='test type'))
        marketplace.add_to_cart(0, Tea(name='Test', price=12, type='test type'))
        marketplace.publish(0, Tea(name='Test 2', price=10, type='test type 2'))
        marketplace.add_to_cart(0, Tea(name='Test 2', price=10, type='test type 2'))

        ref = [
            Tea(name='Test', price=12, type='test type'),
            Tea(name='Test 2', price=10, type='test type 2')
            ]

        self.assertEqual(marketplace.place_order(0), ref)

    def test_get_print_lock(self):
        marketplace = Marketplace(5)
        self.assertEqual(marketplace.get_print_lock(), marketplace.print_lock)

if __name__ == '__main__':
    unittest.main()
