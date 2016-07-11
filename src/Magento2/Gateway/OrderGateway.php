<?php
/**
 * Magento2\Gateway\OrderGateway
 * @category Magento2
 * @package Magento2\Gateway
 * @author Matt Johnston
 * @author Andreas Gerhards <andreas@lero9.co.nz>
 * @copyright Copyright (c) 2014 LERO9 Ltd.
 * @license Commercial - All Rights Reserved
 */

namespace Magento2\Gateway;

use Entity\Comment;
use Entity\Service\EntityService;
use Entity\Wrapper\Order;
use Entity\Wrapper\Orderitem;
use Log\Service\LogService;
use Magelink\Exception\MagelinkException;
use Magelink\Exception\NodeException;
use Magelink\Exception\GatewayException;
use Magento2\Service\Magento2ConfigService;
use Node\AbstractNode;
use Zend\Stdlib\ArrayObject;


class OrderGateway extends AbstractGateway
{
    const GATEWAY_ENTITY_CODE = 'o';
    const GATEWAY_ENTITY = 'order';

    const MAGENTO_STATUS_PENDING = 'pending';
    const MAGENTO_STATUS_PENDING_ALIPAY = 'pending_alipay';
    const MAGENTO_STATUS_PENDING_ALIPAY_NEW = 'new';
    const MAGENTO_STATUS_PENDING_DPS = 'pending_dps';
    const MAGENTO_STATUS_PENDING_OGONE = 'pending_ogone';
    const MAGENTO_STATUS_PENDING_PAYMENT = 'pending_payment';
    const MAGENTO_STATUS_PENDING_PAYPAL = 'pending_paypal';
    const MAGENTO_STATUS_PAYMENT_REVIEW = 'payment_review';
    const MAGENTO_STATUS_FRAUD = 'fraud';
    const MAGENTO_STATUS_FRAUD_DPS = 'fraud_dps';

    private static $magentoPendingStatusses = array(
        self::MAGENTO_STATUS_PENDING,
        self::MAGENTO_STATUS_PENDING_ALIPAY,
        self::MAGENTO_STATUS_PENDING_ALIPAY_NEW,
        self::MAGENTO_STATUS_PENDING_DPS,
        self::MAGENTO_STATUS_PENDING_OGONE,
        self::MAGENTO_STATUS_PENDING_PAYMENT,
        self::MAGENTO_STATUS_PENDING_PAYPAL,
        self::MAGENTO_STATUS_PAYMENT_REVIEW,
        self::MAGENTO_STATUS_FRAUD,
        self::MAGENTO_STATUS_FRAUD_DPS
    );

    const MAGENTO_STATUS_ONHOLD = 'holded';

    const MAGENTO_STATUS_PROCESSING = 'processing';
    const MAGENTO_STATUS_PROCESSING_DPS_PAID = 'processing_dps_paid';
    const MAGENTO_STATUS_PROCESSING_OGONE = 'processed_ogone';
    const MAGENTO_STATUS_PROCESSING_DPS_AUTH = 'processing_dps_auth';
    const MAGENTO_STATUS_PAYPAL_CANCELED_REVERSAL = 'paypal_canceled_reversal';

    private static $magentoProcessingStatusses = array(
        self::MAGENTO_STATUS_PROCESSING,
        self::MAGENTO_STATUS_PROCESSING_DPS_PAID,
        self::MAGENTO_STATUS_PROCESSING_OGONE,
        self::MAGENTO_STATUS_PROCESSING_DPS_AUTH,
        self::MAGENTO_STATUS_PAYPAL_CANCELED_REVERSAL
    );

    const MAGENTO_STATUS_PAYPAL_REVERSED = 'paypal_reversed';

    const MAGENTO_STATUS_COMPLETE = 'complete';
    const MAGENTO_STATUS_CLOSED = 'closed';
    const MAGENTO_STATUS_CANCELED = 'canceled';

    private static $magentoFinalStatusses = array(
        self::MAGENTO_STATUS_COMPLETE,
        self::MAGENTO_STATUS_CLOSED,
        self::MAGENTO_STATUS_CANCELED
    );

    /** @var array $notRetrievedOrderIncrementIds */
    protected $notRetrievedOrderIncrementIds = NULL;


    /**
     * Initialize the gateway and perform any setup actions required.
     * @param string $entityType
     * @return bool $success
     * @throws MagelinkException
     * @throws GatewayException
     */
    protected function _init($entityType)
    {
        $success = parent::_init($entityType);

        if ($entityType != 'order') {
            throw new GatewayException('Invalid entity type for this gateway');
            $success = FALSE;
        }

        return $success;
    }

    /**
     * Get last retrieve date from the database
     * @return bool|string
     */
    protected function getRetrieveDateForForcedSynchronisation()
    {
        if ($this->newRetrieveTimestamp !== NULL) {
            $retrieveInterval = $this->newRetrieveTimestamp - $this->getLastRetrieveTimestamp();
            $intervalsBefore = 2.4 - min(1.2, max(0, $retrieveInterval / 3600));
            $retrieveTimestamp = intval($this->getLastRetrieveTimestamp()
                - min($retrieveInterval * $intervalsBefore, $retrieveInterval + 3600));
            $date = $this->convertTimestampToExternalDateFormat($retrieveTimestamp);
        }else{
            $date = FALSE;
        }

        return $date;
    }

    /**
     * Check, if the order should be ignored or imported
     * @param array $orderData
     * @return bool
     */
    protected function isOrderToBeRetrieved(array $orderData)
    {
        $retrieve = TRUE;

        /** @var Magento2ConfigService $magento2ConfigService */
        $magento2ConfigService = $this->getServiceLocator()->get('magento2ConfigService');

        foreach ($magento2ConfigService->getStoreLimits() as $storeId=>$limits) {
            list($from, $to) = $limits;
            if ($orderData['store_id'] == $storeId) {
                if (intval($orderData['increment_id']) <= $from || intval($orderData['increment_id']) >= $to) {
                    $retrieve = FALSE;
                    break;
                }
            }
        }

        $limits = $magento2ConfigService->getStoreLimitForPendingProcessing();
        if (!$retrieve && count($limits) == 2) {
            list($from, $to) = $limits;
            if (intval($orderData['increment_id']) > $from || intval($orderData['increment_id']) < $to) {
                $isOrderPending = self::hasOrderStatePending($orderData['status']);
                $isOrderProcessing = self::hasOrderStateProcessing($orderData['status']);
                $retrieve = $isOrderPending || $isOrderProcessing;
            }
        }

        return $retrieve;
    }

    /**
     * @param $orderStatus
     * @return bool
     */
    public static function hasOrderStatePending($orderStatus)
    {
        $hasOrderStatePending = in_array($orderStatus, self::$magentoPendingStatusses);
        return $hasOrderStatePending;
    }

    /**
     * @param $orderStatus
     * @return bool
     */
    public static function hasOrderStateProcessing($orderStatus)
    {
        $hasOrderStateProcessing = in_array($orderStatus, self::$magentoProcessingStatusses);
        return $hasOrderStateProcessing;
    }

    /**
     * @param Order|int $orderOrStoreId
     * @return int $storeId
     */
    protected function getEntityStoreId($orderOrStoreId, $global)
    {
        if (is_int($orderOrStoreId)) {
            $storeId = $orderOrStoreId;
        }elseif (is_object($orderOrStoreId) && substr(strrchr(get_class($orderOrStoreId), '\\'), 1) == 'Order') {
            $order = $orderOrStoreId;
            $storeId = $order->getStoreId();
        }else{
            $storeId = NULL;
        }

        if ($global || !$this->_node->isMultiStore()) {
            $storeId = 0;
        }

        return $storeId;
    }

    /**
     * @param Order|int $orderOrStoreId
     * @return int $storeId
     */
    protected function getCustomerStoreId($orderOrStoreId)
    {
        $globalCustomer = TRUE;
        return $this->getEntityStoreId($orderOrStoreId, $globalCustomer);
    }

    /**
     * @param Order|int $orderOrStoreId
     * @return int $storeId
     */
    protected function getStockStoreId($orderOrStoreId)
    {
        $globalStock = TRUE;
        return $this->getEntityStoreId($orderOrStoreId, $globalStock);
    }

    /**
     * @param Order $order
     * @param Orderitem $orderitem
     * @return bool|NULL
     * @throws MagelinkException
     */
    protected function updateStockQuantities(Order $order, Orderitem $orderitem)
    {
        $qtyPreTransit = NULL;
        $orderStatus = $order->getData('status');
        $isOrderPending = self::hasOrderStatePending($orderStatus);
        $isOrderProcessing = self::hasOrderStateProcessing($orderStatus);
        $isOrderCancelled = $orderStatus == self::MAGENTO_STATUS_CANCELED;

        $logData = array('order id'=>$order->getId(), 'orderitem'=>$orderitem->getId(), 'sku'=>$orderitem->getData('sku'));
        $logEntities = array('node'=>$this->_node, 'order'=>$order, 'orderitem'=>$orderitem);

        if ($isOrderPending || $isOrderProcessing || $isOrderCancelled) {
            $storeId = $this->getStockStoreId($order);
            $logData['store_id'] = $storeId;

            $stockitem = $this->_entityService->loadEntity(
                $this->_node->getNodeId(),
                'stockitem',
                $storeId,
                $orderitem->getData('sku')
            );
            $logEntities['stockitem'] = $stockitem;

            $success = FALSE;
            if ($stockitem) {
                if ($isOrderProcessing) {
                    $attributeCode = 'qty_pre_transit';
                }else{
                    $attributeCode = 'available';
                }

                $attributeValue = $stockitem->getData($attributeCode, 0);
                $itemQuantity = $orderitem->getData('quantity', 0);
                if ($isOrderPending) {
                    $itemQuantity *= -1;
                }

                $updateData = array($attributeCode=>($attributeValue + $itemQuantity));
                $logData = array_merge($logData, array('quantity'=>$itemQuantity), $updateData);

                try{
                    $this->_entityService->updateEntity($this->_node->getNodeId(), $stockitem, $updateData, FALSE);
                    $success = TRUE;

                    $this->getServiceLocator()->get('logService')
                        ->log(LogService::LEVEL_INFO,
                            $this->getLogCode().'_pre_upd',
                            'Updated '.$attributeCode.' on stockitem '.$stockitem->getEntityId(),
                            $logData, $logEntities
                        );
                }catch (\Exception $exception) {
                    $this->getServiceLocator()->get('logService')
                        ->log(LogService::LEVEL_ERROR,
                            $this->getLogCode().'_si_upd_err',
                            'Update of '.$attributeCode.' failed on stockitem '.$stockitem->getEntityId(),
                            $logData, $logEntities
                        );
                }
            }else{
                $this->getServiceLocator()->get('logService')
                    ->log(LogService::LEVEL_ERROR,
                        $this->getLogCode().'_si_no_ex',
                        'Stockitem '.$orderitem->getData('sku').' does not exist.',
                        $logData, $logEntities
                    );
            }
        }else{
            $this->getServiceLocator()->get('logService')
                ->log(LogService::LEVEL_DEBUGEXTRA,
                    $this->getLogCode().'_upd_pre_f',
                    'No update of qty_pre_transit. Order '.$order->getUniqueId().' has wrong status: '.$orderStatus,
                    array('order id'=>$order->getId()),
                    $logData, $logEntities
                );
            $success = NULL;
        }

        return $success;
    }

    /**
     * Store order with provided order data
     * @param array $orderData
     * @param bool $forced
     * @throws GatewayException
     * @throws MagelinkException
     * @throws NodeException
     */
    protected function storeOrderData(array $orderData, $forced = FALSE)
    {
        if ($forced) {
            $logLevel = LogService::LEVEL_WARN;
            $logCodeSuffix = '_fcd';
            $logMessageSuffix = ' (out of sync - forced)';
        }else{
            $logLevel = LogService::LEVEL_INFO;
            $logMessageSuffix = $logCodeSuffix = '';
        }
        $correctionHours = sprintf('%+d hours', intval($this->_node->getConfig('time_correction_order')));

        $storeId = ($this->_node->isMultiStore() ? $orderData['store_id'] : 0);
        $uniqueId = $orderData['increment_id'];
        $localId = isset($orderData['entity_id']) ? $orderData['entity_id'] : $orderData['order_id'];
        $createdAtTimestamp = strtotime($orderData['created_at']);

        $data = array(
            'customer_email'=>array_key_exists('customer_email', $orderData)
                ? $orderData['customer_email'] : NULL,
            'customer_name'=>(
                array_key_exists('customer_firstname', $orderData) ? $orderData['customer_firstname'].' ' : '')
                .(array_key_exists('customer_lastname', $orderData) ? $orderData['customer_lastname'] : ''
                ),
            'status'=>$orderData['status'],
            'placed_at'=>date('Y-m-d H:i:s', strtotime($correctionHours, $createdAtTimestamp)),
            'grand_total'=>$orderData['base_grand_total'],
            'base_to_currency_rate'=>$orderData['base_to_order_rate'],
            'weight_total'=>(array_key_exists('weight', $orderData)
                ? $orderData['weight'] : 0),
            'discount_total'=>(array_key_exists('base_discount_amount', $orderData)
                ? $orderData['base_discount_amount'] : 0),
            'shipping_total'=>(array_key_exists('base_shipping_amount', $orderData)
                ? $orderData['base_shipping_amount'] : 0),
            'tax_total'=>(array_key_exists('base_tax_amount', $orderData)
                ? $orderData['base_tax_amount'] : 0),
            'shipping_method'=>(array_key_exists('shipping_method', $orderData)
                ? $orderData['shipping_method'] : NULL)
        );

        if (array_key_exists('base_gift_cards_amount', $orderData)) {
            $data['giftcard_total'] = $orderData['base_gift_cards_amount'];
        }elseif (array_key_exists('base_gift_cards_invoiced', $orderData)) {
            $data['giftcard_total'] = $orderData['base_gift_cards_invoiced'];
        }else{
            $data['giftcard_total'] = 0;
        }
        if (array_key_exists('base_reward_currency_amount', $orderData)) {
            $data['reward_total'] = $orderData['base_reward_currency_amount'];
        }elseif (array_key_exists('base_reward_currency_amount_invoiced', $orderData)) {
            $data['reward_total'] = $orderData['base_reward_currency_amount_invoiced']; // database field base_rwrd_crrncy_amt_invoiced
        }else{
            $data['reward_total'] = 0;
        }
        if (array_key_exists('base_customer_balance_amount', $orderData)) {
            $data['storecredit_total'] = $orderData['base_customer_balance_amount'];
        }elseif (array_key_exists('base_customer_balance_amount_invoiced', $orderData)) {
            $data['storecredit_total'] = $orderData['base_customer_balance_amount_invoiced'];
        }else{
            $data['storecredit_total'] = 0;
        }

        $payments = array();
        if (isset($orderData['payment'])) {
            if (is_array($orderData['payment']) && !isset($orderData['payment']['payment_id'])) {
                foreach ($orderData['payment'] as $payment) {
                    $payments = $this->_entityService->convertPaymentData(
                        $payment['method'], $payment['base_amount_ordered'], $payment['cc_type']);
                }
            }elseif (isset($orderData['payment']['payment_id'])) {
                $payments = $this->_entityService->convertPaymentData(
                    $orderData['payment']['method'],
                    $orderData['payment']['base_amount_ordered'],
                    (isset($orderData['payment']['cc_type']) ? $orderData['payment']['cc_type'] : '')
                );
            }else{
                // store as sync issue
                throw new GatewayException('Invalid payment details format for order '.$uniqueId);
            }
        }
        if (count($payments)) {
            $data['payment_method'] = $payments;
        }

        if (isset($orderData['customer_id']) && $orderData['customer_id']) {
            $nodeId = $this->_node->getNodeId();
            $customer = $this->_entityService
                ->loadEntityLocal($nodeId, 'customer', $this->getCustomerStoreId($storeId), $orderData['customer_id']);
                //->loadEntity($nodeId, 'customer', $this->getCustomerStoreId($storeId), $orderData['customer_email']);
            if ($customer && $customer->getId()) {
                $data['customer'] = $customer;
            }else{
                $data['customer'] = NULL;
                // @todo : Should never be the case, exception handling neccessary
            }
        }

        $needsUpdate = TRUE;
        $orderComment = FALSE;

        $existingEntity = $this->_entityService->loadEntityLocal(
            $this->_node->getNodeId(),
            'order',
            $storeId,
            $localId
        );

        if (!$existingEntity) {
            $existingEntity = $this->_entityService->loadEntity(
                $this->_node->getNodeId(),
                'order',
                $storeId,
                $uniqueId
            );

            if (!$existingEntity) {
                $this->_entityService->beginEntityTransaction('magento2-order-'.$uniqueId);
                try{
                    $data = array_merge(
                        $this->createAddresses($orderData),
                        $data
                    );
                    $existingEntity = $this->_entityService->createEntity(
                        $this->_node->getNodeId(),
                        'order',
                        $storeId,
                        $uniqueId,
                        $data,
                        NULL
                    );
                    $this->_entityService->linkEntity($this->_node->getNodeId(), $existingEntity, $localId);

                    $orderComment = array('Initial sync'=>'Order #'.$uniqueId.' synced to Magelink.');

                    $this->getServiceLocator()->get('logService')->log($logLevel,
                            $this->getLogCode().'_new'.$logCodeSuffix,
                            'New order '.$uniqueId.$logMessageSuffix,
                            array('sku'=>$uniqueId),
                            array('node'=>$this->_node, 'entity'=>$existingEntity)
                        );

                    $this->createItems($orderData, $existingEntity);

                    try{
                        $comment = 'Order retrieved by MageLink, Entity #'.$existingEntity->getId();
                        $this->restV1->post('orders/'.$localId.'/comments', array(
                            'statusHistory'=>array(
                                'comment'=>$comment,
                                'isCustomerNotified'=>0,
                                'isVisibleOnFront'=>0,
                                'parentId'=>$localId
                            )
                        ));
                    }catch (\Exception $exception) {
                        $this->getServiceLocator()->get('logService')->log($logLevel,
                                $this->getLogCode().'_r_cerr'.$logCodeSuffix,
                                'Failed to write comment on order '.$uniqueId.$logMessageSuffix,
                                array('exception message'=>$exception->getMessage()),
                                array('node'=>$this->_node, 'entity'=>$existingEntity, 'exception'=>$exception)
                            );
                    }
                    $this->_entityService->commitEntityTransaction('magento2-order-'.$uniqueId);
                }catch (\Exception $exception) {
                    $this->_entityService->rollbackEntityTransaction('magento2-order-'.$uniqueId);
                    throw new GatewayException($exception->getMessage(), $exception->getCode(), $exception);
                }
                $needsUpdate = FALSE;
            }else{
                $this->getServiceLocator()->get('logService')->log($logLevel,
                    $this->getLogCode().'_unlink'.$logCodeSuffix,
                        'Unlinked order '.$uniqueId.$logMessageSuffix,
                        array('sku'=>$uniqueId),
                        array('node'=>$this->_node, 'entity'=>$existingEntity)
                    );
                $this->_entityService->linkEntity($this->_node->getNodeId(), $existingEntity, $localId);
            }
        }else{
            $attributesNotToUpdate = array('grand_total');
            foreach ($attributesNotToUpdate as $code) {
                if ($existingEntity->getData($code, NULL) !== NULL) {
                    unset($data[$code]);
                }
            }
            $this->getServiceLocator()->get('logService')->log($logLevel,
                $this->getLogCode().'_upd'.$logCodeSuffix,
                    'Updated order '.$uniqueId.$logMessageSuffix,
                    array('order'=>$uniqueId),
                    array('node'=>$this->_node, 'entity'=>$existingEntity)
                );
        }

        if ($needsUpdate) {
            try{
                $oldStatus = $existingEntity->getData('status', NULL);
                $statusChanged = $oldStatus != $data['status'];
                if (!$orderComment && $statusChanged) {
                    $orderComment = array(
                        'Status change'=>'Order #'.$uniqueId.' moved from '.$oldStatus.' to '.$data['status']
                    );
                }

                $movedToProcessing = self::hasOrderStateProcessing($orderData['status'])
                    && !self::hasOrderStateProcessing($existingEntity->getData('status'));
                $movedToCancel = $orderData['status'] == self::MAGENTO_STATUS_CANCELED
                    && $existingEntity->getData('status') != self::MAGENTO_STATUS_CANCELED;
                $this->_entityService->updateEntity($this->_node->getNodeId(), $existingEntity, $data, FALSE);

                $order = $this->_entityService->loadEntityId($this->_node->getNodeId(), $existingEntity->getId());
                if ($movedToProcessing || $movedToCancel) {
                    /** @var Order $order */
                    foreach ($order->getOrderitems() as $orderitem) {
                        $this->updateStockQuantities($order, $orderitem);
                    }
                }
            }catch (\Exception $exception) {
                throw new GatewayException('Needs update: '.$exception->getMessage(), 0, $exception);
            }
        }

        try{
            if ($orderComment) {
                if (!is_array($orderComment)) {
                    $orderComment = array($orderComment=>$orderComment);
                }
                $this->_entityService
                    ->createEntityComment($existingEntity, 'Magento2', key($orderComment), current($orderComment));
            }
        }catch (\Exception $exception) {
            $this->getServiceLocator()->get('logService')
                ->log(LogService::LEVEL_ERROR,
                    $this->getLogCode().'_w_cerr'.$logCodeSuffix,
                    'Comment creation failed on order '.$uniqueId.'.',
                    array('order'=>$uniqueId, 'order comment array'=>$orderComment, 'error'=>$exception->getMessage()),
                    array('entity'=>$existingEntity, 'exception'=>$exception)
                );
        }

        try{
            $this->updateStatusHistory($orderData, $existingEntity);
        }catch (\Exception $exception) {
            $this->getServiceLocator()->get('logService')
                ->log(LogService::LEVEL_ERROR,
                    $this->getLogCode().'_w_herr'.$logCodeSuffix,
                    'Updating of the status history failed on order '.$uniqueId.'.',
                    array('order'=>$uniqueId, 'order data'=>$orderData, 'error'=>$exception->getMessage()),
                    array('entity'=>$existingEntity, 'exception'=>$exception)
                );
        }
    }

    /**
     * Retrieve and action all updated records (either from polling, pushed data, or other sources).
     * @throws GatewayException
     * @throws MagelinkException
     * @throws NodeException
     */
    public function retrieveEntities()
    {
        $this->getServiceLocator()->get('logService')
            ->log(LogService::LEVEL_INFO,
                $this->getLogCode().'_re_time',
                'Retrieving orders updated since '.$this->lastRetrieveDate,
                array('type'=>'order', 'timestamp'=>$this->lastRetrieveDate)
            );

        $success = NULL;
        if (FALSE && $this->db) {
            try{
                // @todo (maybe): Implement
                $storeId = $orderIds = FALSE;
                $orders = $this->db->getOrders($storeId, $this->lastRetrieveDate, FALSE, $orderIds);
                foreach ($orders as $orderData) {
                    $orderData = (array) $orderData;
                    if ($this->isOrderToBeRetrieved($orderData)) {
                        $success = $this->storeOrderData($orderData);
                    }
                }
            }catch (\Exception $exception) {
                // store as sync issue
                throw new GatewayException($exception->getMessage(), $exception->getCode(), $exception);
            }
        }elseif ($this->restV1) {
            try{
                $filter = array(array(
                    'field'=>'updated_at',
                    'value'=>$this->lastRetrieveDate,
                    'condition_type'=>'gt'
                ));
                $orders = $this->restV1->get('orders', array('filter'=>$filter));

                $this->getServiceLocator()->get('logService')
                    ->log(LogService::LEVEL_INFO,
                        $this->getLogCode().'_rest_list',
                        'Retrieved salesOrderList updated from '.$this->lastRetrieveDate,
                        array('updated_at'=>$this->lastRetrieveDate, 'orders'=>$orders)
                    );
                foreach ($orders as $orderData) {
                    if ($this->isOrderToBeRetrieved($orderData)) {
                        $this->storeOrderData($orderData);
                    }
                }
            }catch(\Exception $exception) {
                if (!isset($orders) || !$orders) {
                    $this->getServiceLocator()->get('logService')
                        ->log(LogService::LEVEL_ERROR,
                            $this->getLogCode().'rest_lerr',
                            'Error on restV1 call salesOrderList since '.$this->lastRetrieveDate,
                            array('orders'=>(isset($orders) ? $orders : 'not set'), 'filter'=>$filter)
                        );
                }
                // store as sync issue
                throw new GatewayException($exception->getMessage(), $exception->getCode(), $exception);
            }
        }else{
            throw new NodeException('No valid API available for sync');
        }

        $this->_nodeService
            ->setTimestamp($this->_nodeEntity->getNodeId(), 'order', 'retrieve', $this->getNewRetrieveTimestamp());

        $seconds = ceil($this->getAdjustedTimestamp() - $this->getNewRetrieveTimestamp());
        $message = 'Retrieved '.count($orders).' orders in '.$seconds.'s up to '
            .strftime('%H:%M:%S, %d/%m', $this->retrieveTimestamp).'.';
        $logData = array('type'=>'order', 'amount'=>count($orders), 'period [s]'=>$seconds);
        if (count($orders) > 0) {
            $logData['per entity [s]'] = round($seconds / count($orders), 3);
        }
        $this->getServiceLocator()->get('logService')
            ->log(LogService::LEVEL_INFO, $this->getLogCode().'_re_no', $message, $logData);

        try{
            $this->forceSynchronisation();
        }catch(\Exception $exception) {
            // store as sync issue
           throw new GatewayException($exception->getMessage(), $exception->getCode(), $exception);
        }
    }

    /**
     * Compare orders on Magento2 with the orders no Magelink and return increment id array of orders not retrieved
     * @return array|bool $notRetrievedOrderIncrementIds
     * @throws GatewayException
     * @throws NodeException
     */
    protected function getNotRetrievedOrders()
    {
        $start = microtime(TRUE);
        $logCode = $this->getLogCode().'_re_f';

        if ($this->notRetrievedOrderIncrementIds === NULL) {
            $notRetrievedOrderIncrementIds = array();

            if ($this->db) {
                $api = 'db';
                try {
                    $orders = $this->db->getOrders(
                        FALSE,
                        $this->getRetrieveDateForForcedSynchronisation(),
                        $this->convertTimestampToExternalDateFormat($this->getNewRetrieveTimestamp())
                    );
                }catch (\Exception $exception) {
                    // store as sync issue
                    throw new GatewayException($exception->getMessage(), $exception->getCode(), $exception);
                }
            }elseif ($this->restV1) {
                $api = 'restV1';
                if ($this->getRetrieveDateForForcedSynchronisation()) {
                    $filter = array(array(
                        'field'=>'updated_at',
                        'value'=>$this->getRetrieveDateForForcedSynchronisation(),
                        'condition_type'=>'gt'
                    ));
                }else{
                    // All orders
                    $filter = array(array('field'=>'entity_id', 'value'=>0, 'condition_type'=>'gt'));
                }

                try {
                    $orders = $this->restV1->get('orders', $filter);
                }catch (\Exception $exception) {
                    // store as sync issue
                    throw new GatewayException($exception->getMessage(), $exception->getCode(), $exception);
                }
            }else {
                throw new NodeException('No valid API available for synchronisation check');
            }

            foreach ($orders as $magentoOrder) {
                if ($magentoOrder instanceof \ArrayObject) {
                    $magentoOrder = (array) $magentoOrder;
                }
                if ($this->isOrderToBeRetrieved((array) $magentoOrder)) {
                    $isMagelinkOrder = $this->_entityService->isEntity(
                        $this->_nodeEntity->getNodeId(), 'order', 0, $magentoOrder['increment_id']);
                    if (!$isMagelinkOrder) {
                        $notRetrievedOrderIncrementIds[$magentoOrder['entity_id']] = $magentoOrder['increment_id'];
                    }
                }
            }

            if ($notRetrievedOrderIncrementIds) {
                $this->notRetrievedOrderIncrementIds = $notRetrievedOrderIncrementIds;
                $seconds = ceil(microtime(TRUE) - $start);
                $message = 'Get not retrieved orders (back to '.$this->getRetrieveDateForForcedSynchronisation()
                    .') via '.$api.' took '.$seconds.'s.';
                $this->getServiceLocator()->get('logService')
                    ->log(LogService::LEVEL_INFO, $logCode.'_'.$api.'no', $message, array(
                        'checked orders'=>count($orders),
                        'not retrieved orders'=>count($notRetrievedOrderIncrementIds),
                        'retrieve start date'=>$this->getRetrieveDateForForcedSynchronisation(),
                        'period[s]'=>$seconds
                    ));
            }else{
                $this->notRetrievedOrderIncrementIds = FALSE;
            }
        }

        return $this->notRetrievedOrderIncrementIds;
    }

    /**
     * Check if all orders are retrieved from Magento2 into Magelink
     * @return bool
     */
    protected function areOrdersInSync()
    {
        if ($this->notRetrievedOrderIncrementIds === NULL) {
            $this->getNotRetrievedOrders();
        }
        $isInSync = !(bool) $this->notRetrievedOrderIncrementIds;

        return $isInSync;
    }

    /**
     * Check for orders out of sync; load, create and check them; return success/failure
     * @return bool $success
     * @throws GatewayException
     * @throws MagelinkException
     * @throws NodeException
     */
    public function forceSynchronisation()
    {
        $success = TRUE;
        $start = microtime(TRUE);

        if (!$this->areOrdersInSync()) {
            $logCode = $this->getLogCode().'_re_frc';
            $forcedOrders = count($this->notRetrievedOrderIncrementIds);

            $orderOutOfSyncList = implode(', ', $this->notRetrievedOrderIncrementIds);
            $this->getServiceLocator()->get('logService')
                ->log(LogService::LEVEL_WARN, $logCode,
                    'Retrieving orders: '.$orderOutOfSyncList,
                    array(), array('order increment ids out of sync'=>$orderOutOfSyncList)
                );

            foreach ($this->notRetrievedOrderIncrementIds as $localId=>$orderIncrementId) {
                if (FALSE && $this->db) {
                    try {
                        // @todo (maybe): Implemented
                        $orderData = (array) $this->db->getOrderByIncrementId($orderIncrementId);
                    }catch (\Exception $exception) {
                        // store as sync issue
                        throw new GatewayException($exception->getMessage(), $exception->getCode(), $exception);
                    }
                }elseif ($this->restV1) {
                    $orderData = $this->restV1->get('orders/'.$localId, array());
                }else{
                    throw new NodeException('No valid API available for forced synchronisation');
                }

                $this->storeOrderData($orderData, TRUE);

                $magelinkOrder = $this->_entityService
                    ->loadEntity($this->_nodeEntity->getNodeId(), 'order', 0, $orderIncrementId);
                if ($magelinkOrder) {
                    unset($this->notRetrievedOrderIncrementIds[$magelinkOrder->getUniqueId()]);
                }
            }

            $forcedOrders -= count($this->notRetrievedOrderIncrementIds);
            $seconds = ceil(microtime(TRUE) - $start);
            $logData = array('type'=>'order', 'forced orders'=>$forcedOrders, 'period [s]'=>$seconds);
            if (count($forcedOrders) > 0) {
                $logData['per entity [s]'] = round($seconds / count($forcedOrders), 3);
            }

            if (count($this->notRetrievedOrderIncrementIds) > 0) {
                $success = FALSE;
                $orderOutOfSyncList = implode(', ', $this->notRetrievedOrderIncrementIds);

                $logLevel = LogService::LEVEL_ERROR;
                $logCode .= 'err';
                $logMessage = 'Forced retrieval failed for orders: '.$orderOutOfSyncList;
                $logData['order increment ids still out of sync'] = $orderOutOfSyncList;
            }else{
                $logLevel = LogService::LEVEL_INFO;
                $logCode .= 'no';
                $logMessage = 'Forced retrieval on '.$forcedOrders.' orders in '.$seconds.'s up to '
                    .strftime('%H:%M:%S, %d/%m', $this->getNewRetrieveTimestamp()).'.';
            }

            $this->getServiceLocator()->get('logService')->log($logLevel, $logCode, $logMessage, $logData);
        }

        return $success;
    }

    /**
     * Insert any new status history entries as entity comments
     * @param array $orderData The full order data
     * @param Order $orderEntity The order entity to attach to
     * @throws MagelinkException
     * @throws NodeException
     */
    protected function updateStatusHistory(array $orderData, Order $orderEntity)
    {
        $referenceIds = array();
        $commentIds = array();
        $comments = $this->_entityService->loadEntityComments($orderEntity);

        foreach($comments as $com){
            $referenceIds[] = $com->getReferenceId();
            $commentIds[] = $com->getCommentId();
        }

        foreach ($orderData['status_history'] as $historyItem) {
            if (isset($historyItem['comment']) && preg_match('/{([0-9]+)} - /', $historyItem['comment'], $matches)) {
                if(in_array($matches[1], $commentIds)){
                    continue; // Comment already loaded through another means
                }
            }
            if (in_array($historyItem['created_at'], $referenceIds)) {
                continue; // Comment already loaded
            }

            if (!isset($historyItem['comment'])) {
                $historyItem['comment'] = '(no comment)';
            }
            if (!isset($historyItem['status'])) {
                $historyItem['status'] = '(no status)';
            }
            $notifyCustomer = isset($historyItem['is_customer_notified']) && $historyItem['is_customer_notified'] == '1';

            $this->_entityService->createEntityComment(
                $orderEntity,
                'Magento2',
                'Status History Event: '.$historyItem['created_at'].' - '.$historyItem['status'],
                $historyItem['comment'],
                $historyItem['created_at'],
                $notifyCustomer
            );
        }
    }

    /**
     * Create all the OrderItem entities for a given order
     * @param array $orderData
     * @param Order $order
     * @throws MagelinkException
     * @throws NodeException
     */
    protected function createItems(array $orderData, Order $order)
    {
        $nodeId = $this->_node->getNodeId();
        $parentId = $order->getId();

        foreach ($orderData['items'] as $item) {
            $uniqueId = $orderData['increment_id'].'-'.$item['sku'].'-'.$item['item_id'];

            $entity = $this->_entityService
                ->loadEntity(
                    $this->_node->getNodeId(),
                    'orderitem',
                    ($this->_node->isMultiStore() ? $orderData['store_id'] : 0),
                    $uniqueId
                );
            if (!$entity) {
                $localId = $item['item_id'];
                $product = $this->_entityService->loadEntity($this->_node->getNodeId(), 'product', 0, $item['sku']);
                $data = array(
                    'product'=>($product ? $product->getId() : null),
                    'sku'=>$item['sku'],
                    'product_name'=>isset($item['name']) ? $item['name'] : '',
                    'is_physical'=>((isset($item['is_virtual']) && $item['is_virtual']) ? 0 : 1),
                    'product_type'=>(isset($item['product_type']) ? $item['product_type'] : null),
                    'quantity'=>$item['qty_ordered'],
                    'item_price'=>(isset($item['base_price']) ? $item['base_price'] : 0),
                    'total_price'=>(isset($item['base_row_total']) ? $item['base_row_total'] : 0),
                    'total_tax'=>(isset($item['base_tax_amount']) ? $item['base_tax_amount'] : 0),
                    'total_discount'=>(isset($item['base_discount_amount']) ? $item['base_discount_amount'] : 0),
                    'weight'=>(isset($item['row_weight']) ? $item['row_weight'] : 0),
                );

                if (isset($item['base_price_incl_tax'])) {
                    $data['item_tax'] = $item['base_price_incl_tax'] - $data['item_price'];
                }elseif ($data['total_price'] && $data['total_price'] > 0) {
                    $data['item_tax'] = ($data['total_tax'] / $data['total_price']) * $data['item_price'];
                }elseif ($data['quantity'] && $data['quantity'] > 0){
                    $data['item_tax'] = $data['total_tax'] / $data['quantity'];
                }else{
                    $data['item_tax'] = 0;
                }

                $data['item_discount'] = ($data['quantity'] ? $data['total_discount'] / $data['quantity'] : 0);

                $this->getServiceLocator()->get('logService')
                    ->log(LogService::LEVEL_INFO,
                        $this->getLogCode().'_re_cr_oi',
                        'Create item data',
                        array('orderitem uniqued id'=>$uniqueId, 'quantity'=>$data['quantity'],'data'=>$data)
                    );

                $storeId = ($this->_node->isMultiStore() ? $orderData['store_id'] : 0);
                $orderitem = $this->_entityService
                    ->createEntity($nodeId, 'orderitem', $storeId, $uniqueId, $data, $parentId);
                $this->_entityService
                    ->linkEntity($this->_node->getNodeId(), $orderitem, $localId);

                $this->updateStockQuantities($order, $orderitem);
            }
        }

    }

    /**
     * Create the Address entities for a given order and pass them back as the appropraite attributes
     * @param array $orderData
     * @return array $data
     */
    protected function createAddresses(array $orderData)
    {
        $data = array();
        if(isset($orderData['shipping_address'])){
            $data['shipping_address'] = $this->createAddressEntity($orderData['shipping_address'], $orderData, 'shipping');
        }
        if(isset($orderData['billing_address'])){
            $data['billing_address'] = $this->createAddressEntity($orderData['billing_address'], $orderData, 'billing');
        }
        return $data;
    }

    /**
     * Creates an individual address entity (billing or shipping)
     * @param array $addressData
     * @param array $orderData
     * @param string $type "billing" or "shipping"
     * @return Order|null $entity
     * @throws MagelinkException
     * @throws NodeException
     */
    protected function createAddressEntity(array $addressData, array $orderData, $type)
    {
        if (!array_key_exists('address_id', $addressData) || $addressData['address_id'] == NULL) {
            return NULL;
        }

        $uniqueId = 'order-'.$orderData['increment_id'].'-'.$type;

        $entity = $this->_entityService->loadEntity(
            $this->_node->getNodeId(), 'address', ($this->_node->isMultiStore() ? $orderData['store_id'] : 0), $uniqueId
        );
/*
        // DISABLED: Generally doesn't work.
        if (!$entity) {
            $entity = $this->_entityService->loadEntityLocal(
                $this->_node->getNodeId(),
                'address',
                ($this->_node->isMultiStore() ? $orderData['store_id'] : 0),
                $addressData['address_id']
            );
        }
*/
        if (!$entity) {
            $data = array(
                'first_name'=>(isset($addressData['firstname']) ? $addressData['firstname'] : null),
                'last_name'=>(isset($addressData['lastname']) ? $addressData['lastname'] : null),
                'street'=>(isset($addressData['street']) ? $addressData['street'] : null),
                'city'=>(isset($addressData['city']) ? $addressData['city'] : null),
                'region'=>(isset($addressData['region']) ? $addressData['region'] : null),
                'postcode'=>(isset($addressData['postcode']) ? $addressData['postcode'] : null),
                'country_code'=>(isset($addressData['country_id']) ? $addressData['country_id'] : null),
                'telephone'=>(isset($addressData['telephone']) ? $addressData['telephone'] : null),
                'company'=>(isset($addressData['company']) ? $addressData['company'] : null)
            );

            $entity = $this->_entityService->createEntity(
                $this->_node->getNodeId(),
                'address',
                ($this->_node->isMultiStore() ? $orderData['store_id'] : 0),
                $uniqueId,
                $data
            );
            $this->_entityService->linkEntity($this->_node->getNodeId(), $entity, $addressData['address_id']);
        }

        return $entity;
    }

    /**
     * Write out all the updates to the given entity.
     * @param \Entity\Entity $entity
     * @param string[] $attributes
     * @param int $type
     */
    public function writeUpdates(\Entity\Entity $entity, $attributes, $type = \Entity\Update::TYPE_UPDATE)
    {
        // @todo (unlikely): Create method. (We don't perform any direct updates to orders in this manner).
        return;
    }

    /**
     * Write out the given action.
     * @param \Entity\Action $action
     * @return bool $success
     * @throws GatewayException
     * @throws MagelinkException
     * @throws NodeException
     */
    public function writeAction(\Entity\Action $action)
    {
        /** @var \Entity\Wrapper\Order $order */
        $order = $action->getEntity();
        // Reload order because entity might have changed in the meantime
        $order = $this->_entityService->reloadEntity($order);
        $localId = $this->_entityService->getLocalId($this->_node->getNodeId(), $order);
        $orderStatus = $order->getData('status');

        $success = TRUE;
        switch ($action->getType()) {
            case 'comment':
                $status = ($action->hasData('status') ? $action->getData('status') : $orderStatus);
                $comment = $action->getData('comment');
                if ($comment == NULL && $action->getData('body')) {
                    if ($action->getData('title') != NULL) {
                        $comment = $action->getData('title').' - ';
                    }else{
                        $comment = '';
                    }
                    if($action->hasData('comment_id')){
                        $comment .= '{'.$action->getData('comment_id').'} ';
                    }
                    $comment .= $action->getData('body');
                }

                if ($action->hasData('customer_visible')) {
                    $notify = $action->getData('customer_visible') ? 'true' : 'false';
                }else{
                    $notify = ($action->hasData('notify') ? ($action->getData('notify') ? 'true' : 'false' ) : NULL);
                }

                try {
                    $this->restV1->post('orders/'.$localId.'/comments', array(
                        'statusHistory'=>array(
                            'comment'=>$comment,
                            'isCustomerNotified'=>$notify,
                            'isVisibleOnFront'=>0,
                            'parentId'=>$localId
                        )
                    ));
                }catch (\Exception $exception) {
                    // store as sync issue
                    throw new GatewayException($exception->getMessage(), $exception->getCode(), $exception);
                }
                break;
            case 'cancel':
                $isCancelable = self::hasOrderStatePending($orderStatus);
                if ($orderStatus !== self::MAGENTO_STATUS_CANCELED) {
                    if (!$isCancelable){
                        $message = 'Attempted to cancel non-pending order '.$order->getUniqueId().' ('.$orderStatus.')';
                        // store as a sync issue
                        throw new GatewayException($message);
                        $success = FALSE;
                    }elseif ($order->isSegregated()) {
                        // store as a sync issue
                        throw new GatewayException('Attempted to cancel child order '.$order->getUniqueId().' !');
                        $success = FALSE;
                    }else{
                        try {
                            $this->restV1->post('orders/'.$localId.'/cancel', array());
                            // Update status straight away
                            $changedOrder = $this->restV1->get('orders/'.$localId, array());

                            $newStatus = $changedOrder['status'];
                            $changedOrderData = array('status'=>$newStatus);
                            $this->_entityService->updateEntity(
                                $this->_node->getNodeId(),
                                $order,
                                $changedOrderData,
                                FALSE
                            );
                            $changedOrderData['status_history'] = array(array(
                                'comment'=>'Magelink updated status from Magento2 after abandoning order to '.$newStatus.'.',
                                'created_at'=>date('Y/m/d H:i:s')
                            ));
                            $this->updateStatusHistory($changedOrderData, $order);
                        }catch (\Exception $exception) {
                            // store as sync issue
                            throw new GatewayException($exception->getMessage(), $exception->getCode(), $exception);
                        }
                    }
                }
                break;
            case 'hold':
                if ($order->isSegregated()) {
                    // Is that really necessary to throw an exception?
                    throw new GatewayException('Attempted to hold child order!');
                    $success = FALSE;
                }else{
                    try {
                        $this->restV1->post('orders/'.$localId.'/hold ', array());
                    }catch (\Exception $exception) {
                        // store as sync issue
                        throw new GatewayException($exception->getMessage(), $exception->getCode(), $exception);
                    }
                }
                break;
            case 'unhold':
                if ($order->isSegregated()) {
                    // Is that really necessary to throw an exception?
                    throw new GatewayException('Attempted to unhold child order!');
                    $success = FALSE;
                }else{
                    try {
                        $this->restV1->post('orders/'.$localId.'/unhold ', array());
                    }catch (\Exception $exception) {
                        // store as sync issue
                        throw new GatewayException($exception->getMessage(), $exception->getCode(), $exception);
                    }
                }
                break;
            case 'ship':
                if (self::hasOrderStateProcessing($orderStatus)) {
                    $comment = ($action->hasData('comment') ? $action->getData('comment') : NULL);
                    $notify = ($action->hasData('notify') ? ($action->getData('notify') ? 'true' : 'false' ) : NULL);
                    $sendComment = ($action->hasData('send_comment') ?
                        ($action->getData('send_comment') ? 'true' : 'false' ) : NULL);
                    $itemsShipped = ($action->hasData('items') ? $action->getData('items') : NULL);
                    $trackingCode = ($action->hasData('tracking_code') ? $action->getData('tracking_code') : NULL);

                    $this->actionShip($order, $comment, $notify, $sendComment, $itemsShipped, $trackingCode);
                }else{
                    $message = 'Invalid order status for shipment: '
                        .$order->getUniqueId().' has '.$order->getData('status');
                    // Is that really necessary to throw an exception?
                    throw new GatewayException($message);
                    $success = FALSE;
                }
                break;
/*                // @todo (maybe): Implemented creditmemo action
                case 'creditmemo':
                if (self::hasOrderStateProcessing($orderStatus) || $orderStatus == self::MAGENTO_STATUS_COMPLETE) {
                    $comment = ($action->hasData('comment') ? $action->getData('comment') : NULL);
                    $notify = ($action->hasData('notify') ? ($action->getData('notify') ? 'true' : 'false' ) : NULL);
                    $sendComment = ($action->hasData('send_comment') ?
                        ($action->getData('send_comment') ? 'true' : 'false' ) : NULL);
                    $itemsRefunded = ($action->hasData('items') ? $action->getData('items') : NULL);
                    $shippingRefund = ($action->hasData('shipping_refund') ? $action->getData('shipping_refund') : 0);
                    $creditRefund = ($action->hasData('credit_refund') ? $action->getData('credit_refund') : 0);
                    $adjustmentPositive =
                        ($action->hasData('adjustment_positive') ? $action->getData('adjustment_positive') : 0);
                    $adjustmentNegative =
                        ($action->hasData('adjustment_negative') ? $action->getData('adjustment_negative') : 0);

                    $message = 'Magento2, create creditmemo: Passing values orderIncrementId '.$order->getUniqueId()
                        .'creditmemoData: [qtys=>'.var_export($itemsRefunded, TRUE).', shipping_amount=>'.$shippingRefund
                        .', adjustment_positive=>'.$adjustmentPositive.', adjustment_negative=>'.$adjustmentNegative
                        .'], comment '.$comment.', notifyCustomer '.$notify.', includeComment '.$sendComment
                        .', refundToStoreCreditAmount '.$creditRefund.'.';
                    $this->getServiceLocator()->get('logService')
                        ->log(LogService::LEVEL_DEBUGEXTRA,
                            $this->getLogCode().'_wr_cr_cm',
                            $message,
                            array(
                                'entity (order)'=>$order,
                                'action'=>$action,
                                'action data'=>$action->getData(),
                                'orderIncrementId'=>$order->getUniqueId(),
                                'creditmemoData'=>array(
                                    'qtys'=>$itemsRefunded,
                                    'shipping_amount'=>$shippingRefund,
                                    'adjustment_positive'=>$adjustmentPositive,
                                    'adjustment_negative'=>$adjustmentNegative
                                ),
                                'comment'=>$comment,
                                'notifyCustomer'=>$notify,
                                'includeComment'=>$sendComment,
                                'refundToStoreCreditAmount'=>$creditRefund
                            )
                        );
                    $this->actionCreditmemo($order, $comment, $notify, $sendComment,
                        $itemsRefunded, $shippingRefund, $creditRefund, $adjustmentPositive, $adjustmentNegative);
                }else{
                    $message = 'Invalid order status for creditmemo: '.$order->getUniqueId().' has '.$orderStatus;
                    // store as a sync issue
                    throw new GatewayException($message);
                    $success = FALSE;
            }
                break;
*/
            default:
                // store as a sync issue
                throw new GatewayException('Unsupported action type '.$action->getType().' for Magento2 Orders.');
                $success = FALSE;
        }

        return $success;
    }

    /**
     * Preprocesses order items array (key=orderitem entity id, value=quantity) into an array suitable for Magento2
     * (local item ID=>quantity), while also auto-populating if not specified.
     * @param Order $order
     * @param array|NULL $rawItems
     * @return array
     * @throws GatewayException
     */
    protected function preprocessRequestItems(Order $order, $rawItems = NULL)
    {
        $items = array();
        if($rawItems == null){
            $orderItems = $this->_entityService->locateEntity(
                $this->_node->getNodeId(),
                'orderitem',
                $order->getStoreId(),
                array(
                    'PARENT_ID'=>$order->getId(),
                ),
                array(
                    'PARENT_ID'=>'eq'
                ),
                array('linked_to_node'=>$this->_node->getNodeId()),
                array('quantity')
            );
            foreach($orderItems as $oi){
                $localid = $this->_entityService->getLocalId($this->_node->getNodeId(), $oi);
                $items[$localid] = $oi->getData('quantity');
            }
        }else{
            foreach ($rawItems as $entityId=>$quantity) {
                $item = $this->_entityService->loadEntityId($this->_node->getNodeId(), $entityId);
                if ($item->getTypeStr() != 'orderitem' || $item->getParentId() != $order->getId()
                    || $item->getStoreId() != $order->getStoreId()){

                    $message = 'Invalid item '.$entityId.' passed to preprocessRequestItems for order '.$order->getId();
                    throw new GatewayException($message);
                }

                if ($quantity == NULL) {
                    $quantity = $item->getData('quantity');
                }elseif ($quantity > $item->getData('quantity')) {
                    $message = 'Invalid item quantity '.$quantity.' for item '.$entityId.' in order '.$order->getId()
                        .' - max was '.$item->getData('quantity');
                    throw new GatewayExceptionn($message);
                }

                $localId = $this->_entityService->getLocalId($this->_node->getNodeId(), $item);
                $items[$localId] = $quantity;
            }
        }
        return $items;
    }

    /**
     * Handles refunding an order in Magento2
     *
     * @param Order $order
     * @param string $comment Optional comment to append to order
     * @param string $notify String boolean, whether to notify customer
     * @param string $sendComment String boolean, whether to include order comment in notify
     * @param array $itemsRefunded Array of item entity id->qty to refund, or null if automatic (all)
     * @param int $shippingRefund
     * @param int $creditRefund
     * @param int $adjustmentPositive
     * @param int $adjustmentNegative
     * @throws GatewayException
     */
    protected function actionCreditmemo(Order $order, $comment = '', $notify = 'false', $sendComment = 'false',
        $itemsRefunded = NULL, $shippingRefund = 0, $creditRefund = 0, $adjustmentPositive = 0, $adjustmentNegative = 0)
    {
        $items = array();

        if (count($itemsRefunded)) {
            $processItems = $itemsRefunded;
        }else{
            $processItems = array();
            foreach ($order->getOrderitems() as $orderItem) {
                $processItems[$orderItem->getId()] = 0;
            }
        }

        foreach ($this->preprocessRequestItems($order, $processItems) as $orderitemId=>$qty) {
            $localId = $basePrice = $baseRowTotal = 0;
            $items[] = array(
                'order_item_id'=>$localId,
                'qty'=>$qty,
                'base_price'=>$basePrice,
                'base_row_total'=>$baseRowTotal,
            );
        }

        $creditmemoData = array(
            'qtys'=>$items,
            'shipping_amount'=>$shippingRefund,
            'adjustment_positive'=>$adjustmentPositive,
            'adjustment_negative'=>$adjustmentNegative,
        );

        $originalOrder = $order->getOriginalOrder();
        try {
            /** @todo: Implement RestV1 call
            $restResult = $this->restV1->call('salesOrderCreditmemoCreate', array(
                $originalOrder->getUniqueId(),
                $creditmemoData,
                $comment,
                $notify,
                $sendComment,
                $creditRefund
            ));
*/
            $restResult = $this->restV1->post('creditmemo', array('entity'=>$creditmemoData));
        }catch (\Exception $exception) {
            // store as sync issue
            throw new GatewayException($exception->getMessage(), $exception->getCode(), $exception);
        }

        if (is_object($restResult)) {
            $restResult = $restResult->result;
        }elseif (is_array($restResult)) {
            if (isset($restResult['result'])) {
                $restResult = $restResult['result'];
            }else{
                $restResult = array_shift($restResult);
            }
        }

        if (!$restResult) {
            // store as a sync issue
            throw new GatewayException('Failed to get creditmemo ID from Magento2 for order '.$order->getUniqueId());
        }

        try {
            /** @todo: Implement RestV1 call
            $this->restV1->call('salesOrderCreditmemoAddComment',
                array($restResult, 'FOR ORDER: '.$order->getUniqueId(), FALSE, FALSE));
*/
        }catch (\Exception $exception) {
            // store as a sync issue
            throw new GatewayException($exception->getMessage(), $exception->getCode(), $exception);
        }
    }

    /**
     * Handles shipping an order in Magento2
     *
     * @param Order $order
     * @param string $comment Optional comment to append to order
     * @param string $notify String boolean, whether to notify customer
     * @param string $sendComment String boolean, whether to include order comment in notify
     * @param array|null $itemsShipped Array of item entity id->qty to ship, or null if automatic (all)
     * @throws GatewayException
     */
    protected function actionShip(Order $order, $comment = '', $notify = 'false', $sendComment = 'false',
        $itemsShipped = NULL, $trackingCode = NULL)
    {
        $items = array();
        foreach ($this->preprocessRequestItems($order, $itemsShipped) as $localId=>$qty) {
            $items[] = array('order_item_id'=>$localId, 'qty'=>$qty);
        }
        if (count($items) == 0) {
            $items = NULL;
        }

        $originalOrder = $order->getOriginalOrder();
        $orderId = $originalOrder->getId();
        $localId = $this->_entityService->getLocalId($this->_node->getNodeId(), $originalOrder);

        $this->getServiceLocator()->get('logService')
            ->log(LogService::LEVEL_DEBUGEXTRA,
                $this->getLogCode().'_act_ship',
                'Sending shipment for '.$orderId,
                array(
                    'ord'=>$order->getId(),
                    'items'=>$items,
                    'comment'=>$comment,
                    'notify'=>$notify,
                    'sendComment'=>$sendComment
                ),
                array('node'=>$this->_node, 'entity'=>$order)
            );

        try {
            $restResult = $this->restV1->post('shipment', array(
                'entity'=>array(
                    'order_id'=>$orderId,
                    'itemsQty'=>$items,
                    'comment'=>$comment,
                    'email'=>$notify,
                    'includeComment'=>$sendComment
                )
            ));
        }catch (\Exception $exception) {
            // store as sync issue
            throw new GatewayException($exception->getMessage(), $exception->getCode(), $exception);
        }

        if (is_object($restResult)) {
            $restResult = $restResult->shipmentIncrementId;
        }elseif (is_array($restResult)) {
            if (isset($restResult['entity_id'])) {
                $shipmentLocalId = $restResult['entity_id'];
            }else{
                $logMessage = 'No shipment entity id information in the REST response.';
                if (!is_null($trackingCode)) {
                    $logMessage .= ' Cannot add tracking code.';
                }

                $this->getServiceLocator()->get('logService')->log(LogService::LEVEL_ERROR,
                    $this->getLogCode().'_ship_err', $logMessage,
                    array('order'=>$order->getUniqueId(), 'tracking code'=>$trackingCode, 'rest response'=>$restResult)
                );

                // @todo (maybe): Store as a sync issue

                $shipmentLocalId = $trackingCode = NULL;
            }
        }

        if (!is_null($trackingCode)) {
            try {
                $this->restV1->post('shipment/track', array(
                    'entity'=>array(
                        'carrier_code'=>'custom',
                        'order_id'=>$localId,
                        'parent_id'=>$shipmentLocalId,
                        'title'=>$order->getData('shipping_method', 'Shipping'),
                        'trackNumber'=>$trackingCode
                    )
                ));
            }catch (\Exception $exception) {
                // store as sync issue
                throw new GatewayException($exception->getMessage(), $exception->getCode(), $exception);
            }
        }
    }

}