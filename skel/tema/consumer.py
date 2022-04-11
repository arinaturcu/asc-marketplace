"""
This module represents the Consumer.

Computer Systems Architecture Course
Assignment 1
March 2021
"""

from threading import Thread
from time import sleep


class Consumer(Thread):
    """
    Class that represents a consumer.
    """

    def __init__(self, carts, marketplace, retry_wait_time, **kwargs):
        """
        Constructor.

        :type carts: List
        :param carts: a list of add and remove operations

        :type marketplace: Marketplace
        :param marketplace: a reference to the marketplace

        :type retry_wait_time: Time
        :param retry_wait_time: the number of seconds that a producer must wait
        until the Marketplace becomes available

        :type kwargs:
        :param kwargs: other arguments that are passed to the Thread's __init__()
        """
        Thread.__init__(self, **kwargs)

        self.carts = carts
        self.marketplace = marketplace
        self.retry_wait_time = retry_wait_time
        self.cart_ids = []

		# generate an id for each existing cart
        for _ in carts:
            self.cart_ids.append(marketplace.new_cart())

    def run(self):
        for (cart_id, cart) in zip(self.cart_ids, self.carts):
            for action in cart:
				# add or remove the product for <quantity> times
                for _ in range(action['quantity']):
                    if action['type'] == 'add':
						# try untill the product becomes available
                        while not self.marketplace.add_to_cart(cart_id, action['product']):
                            sleep(self.retry_wait_time)
                    if action['type'] == 'remove':
                        self.marketplace.remove_from_cart(cart_id, action['product'])

			# take the products and the print_lock to print what
			# the customer bought without tangling the messages
            products = self.marketplace.place_order(cart_id)
            print_lock = self.marketplace.get_print_lock()

            with print_lock:
                for product in products:
                    print(self.name + ' bought', product)
