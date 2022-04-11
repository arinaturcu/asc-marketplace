"""
This module represents the Marketplace.

Computer Systems Architecture Course
Assignment 1
March 2021
"""


from threading import Lock
import threading


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
        for id in range(self.producer_id_gen + 1):
            if product in self.queue[id]:
                producer_id = id

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
            return False

        self.carts[cart_id].remove((product, producer_id))
        self.queue[producer_id].append(product)

    def place_order(self, cart_id):
        """
        Return a list with all the products in the cart.

        :type cart_id: Int
        :param cart_id: id cart
        """

        cart = map(lambda pair: pair[0], self.carts[cart_id])
        return cart, self.print_lock

import unittest

class TestMarketplace(unittest.TestCase):

    def test_upper(self):
        self.assertEqual('foo'.upper(), 'FOO')

if __name__ == '__main__':
    unittest.main()
