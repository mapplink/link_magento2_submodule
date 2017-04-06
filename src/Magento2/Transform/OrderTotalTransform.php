<?php
/**
 * A custom transform on create or update of grand_total
 * @category Magento2
 * @package Magento2\Transform
 * @author Andreas Gerhards <andreas@lero9.co.nz>
 * @copyright Copyright (c) 2016 LERO9 Ltd.
 * @license http://opensource.org/licenses/BSD-3-Clause BSD-3-Clause - Please view LICENSE.md for more information
 */


namespace Magento2\Transform;

use Router\Transform\AbstractTransform;
use Entity\Wrapper\Order;


class OrderTotalTransform extends AbstractTransform
{

    /**
     * Perform any initialization/setup actions, and check any prerequisites.
     * @return boolean Whether this transform is eligible to run
     */
    protected function _init()
    {
        return $this->_entity->getTypeStr() == 'order';
    }

    /**
     * Apply the transform on any necessary data
     * @return array New data changes to be merged into the update.
     */
    public function _apply()
    {
        $order = $this->_entity;
        $data = $order->getArrayCopy();

        $orderTotal = $data['grand_total'] - $data['shipping_total'];
        foreach (Order::getNonCashPaymentCodes() as $code) {
            $orderTotal += $data[$code];
        }

        return array('order_total'=>$orderTotal);
    }

}
