"""
This module represents the Marketplace.

Computer Systems Architecture Course
Assignment 1
March 2021
"""

import time
import unittest
import logging
from threading import Lock
from logging.handlers import RotatingFileHandler

from tema.product import Coffee, Tea

logging.basicConfig(
    handlers=[RotatingFileHandler('tema/marketplace.log', maxBytes=10000, backupCount=10)],
    format='%(asctime)s - %(message)s',
    level=logging.INFO
    )

logging.Formatter.converter = time.gmtime

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
        self.queue_lock = Lock()

    def register_producer(self):
        """
        Returns an id for the producer that calls this.
        """
        logging.info('Entered register_producer')

        self.producer_id_gen += 1
        self.queue.append([])

        logging.info('Exited register_producer and returned producer id %s', self.producer_id_gen)
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
        logging.info('Entered publish with producer_id=%s and product=%s', producer_id, product)
        if producer_id > self.producer_id_gen:
            logging.info('Producer not registered (in publish)')
            return False

        if len(self.queue[producer_id]) >= self.queue_size_per_producer:
            logging.info('Producer limit exceeded (in publish)')
            return False

        self.queue[producer_id].append(product)
        logging.info('Exited publish with return value True')
        return True

    def new_cart(self):
        """
        Creates a new cart for the consumer

        :returns an int representing the cart_id
        """
        logging.info('Entered new_cart')
        self.carts.append([])
        logging.info('Exited new_cart and returned cart id %s', len(self.carts) - 1)
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
        logging.info('Entered add_to_cart with cart_id=%s product=%s', cart_id, product)
        with self.queue_lock:
            # use the lock so that another thread won't remove
            # the same product from the queue at the same time
            producer_id = -1
            for temp_producer_id in range(self.producer_id_gen + 1):
                if product in self.queue[temp_producer_id]:
                    producer_id = temp_producer_id

            # exit if the product is not in the queue
            if producer_id == -1:
                logging.info('Product not found in queue (in add_to_cart)')
                return False

            self.carts[cart_id].append((product, producer_id))
            self.queue[producer_id].remove(product)

            logging.info('Exited add_to_cart with return value True')
            return True

    def remove_from_cart(self, cart_id, product):
        """
        Removes a product from cart.

        :type cart_id: Int
        :param cart_id: id cart

        :type product: Product
        :param product: the product to remove from cart
        """
        logging.info('Entered remove_to_cart with cart_id=%s product=%s', cart_id, product)

        producer_id = -1
        for pair in self.carts[cart_id]:
            if pair[0] == product:
                producer_id = pair[1]

        # exit if the product is not in the cart
        if producer_id == -1:
            logging.info('Product not in the cart (in remove_from_cart)')
            return

        self.carts[cart_id].remove((product, producer_id))
        self.queue[producer_id].append(product)
        logging.info('Exited remove_from_cart')

    def place_order(self, cart_id):
        """
        Return a list with all the products in the cart.

        :type cart_id: Int
        :param cart_id: id cart
        """
        logging.info('Entered place_order with cart_id=%s', cart_id)
        if len(self.carts) <= cart_id:
            logging.info('Cart doens\'t exist (in place_order)')
            return []

        cart = list(map(lambda pair: pair[0], self.carts[cart_id]))
        logging.info('Exited place_order succesfully')
        return cart

    def get_print_lock(self):
        """
        Return the lock used for printing
        """
        logging.info('Entered get_print_lock')
        return self.print_lock

class TestMarketplace(unittest.TestCase):
    """
    Class for testing the Marketplace module
    """

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
