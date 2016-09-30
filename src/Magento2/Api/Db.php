<?php
/**
 * Implements DB access to Magento2 - loading and updating
 * @category Magento2
 * @package Magento2\Api
 * @author Andreas Gerhards <andreas@lero9.co.nz>
 * @copyright Copyright (c) 2014 LERO9 Ltd.
 * @license Commercial - All Rights Reserved
 */

namespace Magento2\Api;

use Log\Service\LogService;
use Magelink\Exception\MagelinkException;
use Magento2\Node;
use Zend\Db\Adapter\Adapter;
use Zend\Db\Sql\Delete;
use Zend\Db\Sql\Select;
use Zend\Db\Sql\Where;
use Zend\Db\TableGateway\TableGateway;
use Zend\ServiceManager\ServiceLocatorAwareInterface;
use Zend\ServiceManager\ServiceLocatorInterface;


class Db implements ServiceLocatorAwareInterface
{
    /** @var bool $this->debug */
    protected $debug = TRUE;

    /** @var \Magento2\Node $this->node */
    protected $node;
    /** @var Adapter $this->adapter */
    protected $adapter;
    /** @var array $this->tableGateways */
    protected $tableGateways = array();
    /** @var array $this->entityTypes */
    protected $entityTypes = array();
    /** @var array $this->attributesByEntityType */
    protected $attributesByEntityType = array();
    /** @var bool $this->isEE */
    protected $isEE = FALSE;
    /** @var array $this->orderColumns */
    protected $orderColumns = array(
        'entity_id',
        'state',
        'status',
        'coupon_code',
        'protect_code',
        'shipping_description',
        'is_virtual',
        'store_id',
        'customer_id',
        'base_discount_amount',
        'base_discount_canceled',
        'base_discount_invoiced',
        'base_discount_refunded',
        'base_grand_total',
        'base_shipping_amount',
        'base_shipping_canceled',
        'base_shipping_invoiced',
        'base_shipping_refunded',
        'base_shipping_tax_amount',
        'base_shipping_tax_refunded',
        'base_subtotal',
        'base_subtotal_canceled',
        'base_subtotal_invoiced',
        'base_subtotal_refunded',
        'base_tax_amount',
        'base_tax_canceled',
        'base_tax_invoiced',
        'base_tax_refunded',
        'base_to_global_rate',
        'base_to_order_rate',
        'base_total_canceled',
        'base_total_invoiced',
        'base_total_invoiced_cost',
        'base_total_offline_refunded',
        'base_total_online_refunded',
        'base_total_paid',
        'base_total_qty_ordered',
        'base_total_refunded',
        'discount_amount',
        'discount_canceled',
        'discount_invoiced',
        'discount_refunded',
        'grand_total',
        'shipping_amount',
        'shipping_canceled',
        'shipping_invoiced',
        'shipping_refunded',
        'shipping_tax_amount',
        'shipping_tax_refunded',
        'store_to_base_rate',
        'store_to_order_rate',
        'subtotal',
        'subtotal_canceled',
        'subtotal_invoiced',
        'subtotal_refunded',
        'tax_amount',
        'tax_canceled',
        'tax_invoiced',
        'tax_refunded',
        'total_canceled',
        'total_invoiced',
        'total_offline_refunded',
        'total_online_refunded',
        'total_paid',
        'total_qty_ordered',
        'total_refunded',
        'can_ship_partially',
        'can_ship_partially_item',
        'customer_is_guest',
        'customer_note_notify',
        'billing_address_id',
        'customer_group_id',
        'edit_increment',
        'email_sent',
        'send_email',
        'forced_shipment_with_invoice',
        'payment_auth_expiration',
        'quote_address_id',
        'quote_id',
        'shipping_address_id',
        'adjustment_negative',
        'adjustment_positive',
        'base_adjustment_negative',
        'base_adjustment_positive',
        'base_shipping_discount_amount',
        'base_subtotal_incl_tax',
        'base_total_due',
        'payment_authorization_amount',
        'shipping_discount_amount',
        'subtotal_incl_tax',
        'total_due',
        'weight',
        'customer_dob',
        'increment_id',
        'applied_rule_ids',
        'base_currency_code',
        'customer_email',
        'customer_firstname',
        'customer_lastname',
        'customer_middlename',
        'customer_prefix',
        'customer_suffix',
        'customer_taxvat',
        'discount_description',
        'ext_customer_id',
        'ext_order_id',
        'global_currency_code',
        'hold_before_state',
        'hold_before_status',
        'order_currency_code',
        'original_increment_id',
        'relation_child_id',
        'relation_child_real_id',
        'relation_parent_id',
        'relation_parent_real_id',
        'remote_ip',
        'shipping_method',
        'store_currency_code',
        'store_name',
        'x_forwarded_for',
        'customer_note',
        'created_at',
        'updated_at',
        'total_item_count',
        'customer_gender',
        'discount_tax_compensation_amount',
        'base_discount_tax_compensation_amount',
        'shipping_discount_tax_compensation_amount',
        'base_shipping_discount_tax_compensation_amnt',
        'discount_tax_compensation_invoiced',
        'base_discount_tax_compensation_invoiced',
        'discount_tax_compensation_refunded',
        'base_discount_tax_compensation_refunded',
        'shipping_incl_tax',
        'base_shipping_incl_tax',
        'coupon_rule_name',
        'paypal_ipn_customer_notified',
        'gift_message_id'
    );

    /** @var ServiceLocatorInterface $_serviceLocator */
    protected $_serviceLocator;

    /**
     * Set service locator
     * @param ServiceLocatorInterface $serviceLocator
     */
    public function setServiceLocator(ServiceLocatorInterface $serviceLocator)
    {
        $this->_serviceLocator = $serviceLocator;
    }

    /**
     * Get service locator
     * @return ServiceLocatorInterface
     */
    public function getServiceLocator()
    {
        return $this->_serviceLocator;
    }

    /**
     * Initialize the DB API
     * @param Node $magento2Node
     * @return bool Whether we succeeded
     */
    public function init(Node $magento2Node)
    {
        $success = TRUE;

        $this->node = $magento2Node;
        $this->isEE = $magento2Node->getConfig('enterprise');

        $hostname = $magento2Node->getConfig('db_hostname');
        $schema = $magento2Node->getConfig('db_schema');
        $username = $magento2Node->getConfig('db_username');
        $password = $magento2Node->getConfig('db_password');

        if (!$schema || !$hostname) {
            $success = FALSE;
        }else{
            try{
                $this->adapter = new Adapter(
                    array(
                        'driver'=>'Pdo',
                        'dsn'=>'mysql:host='.$hostname.';dbname='.$schema,
                        'username'=>$username,
                        'password'=>$password,
                        'driver_options'=>array(\PDO::MYSQL_ATTR_INIT_COMMAND=>"SET NAMES 'UTF8'")
                    )
                );
                $this->adapter->getCurrentSchema();

                if ($this->isEE) {
                    $this->orderColumns = array_merge(
                        $this->orderColumns,
                        array(
                            'base_customer_balance_amount',
                            'customer_balance_amount',
                            'base_customer_balance_invoiced',
                            'customer_balance_invoiced',
                            'base_customer_balance_refunded',
                            'customer_balance_refunded',
                            'bs_customer_bal_total_refunded',
                            'customer_bal_total_refunded',
                            'gift_cards',
                            'base_gift_cards_amount',
                            'gift_cards_amount',
                            'base_gift_cards_invoiced',
                            'gift_cards_invoiced',
                            'base_gift_cards_refunded',
                            'gift_cards_refunded',
                            'gw_id',
                            'gw_allow_gift_receipt',
                            'gw_add_card',
                            'gw_base_price',
                            'gw_price',
                            'gw_items_base_price',
                            'gw_items_price',
                            'gw_card_base_price',
                            'gw_card_price',
                            'gw_base_tax_amount',
                            'gw_tax_amount',
                            'gw_items_base_tax_amount',
                            'gw_items_tax_amount',
                            'gw_card_base_tax_amount',
                            'gw_card_tax_amount',
                            'gw_base_price_incl_tax',
                            'gw_price_incl_tax',
                            'gw_items_base_price_incl_tax',
                            'gw_items_price_incl_tax',
                            'gw_card_base_price_incl_tax',
                            'gw_card_price_incl_tax',
                            'gw_base_price_invoiced',
                            'gw_price_invoiced',
                            'gw_items_base_price_invoiced',
                            'gw_items_price_invoiced',
                            'gw_card_base_price_invoiced',
                            'gw_card_price_invoiced',
                            'gw_base_tax_amount_invoiced',
                            'gw_tax_amount_invoiced',
                            'gw_items_base_tax_invoiced',
                            'gw_items_tax_invoiced',
                            'gw_card_base_tax_invoiced',
                            'gw_card_tax_invoiced',
                            'gw_base_price_refunded',
                            'gw_price_refunded',
                            'gw_items_base_price_refunded',
                            'gw_items_price_refunded',
                            'gw_card_base_price_refunded',
                            'gw_card_price_refunded',
                            'gw_base_tax_amount_refunded',
                            'gw_tax_amount_refunded',
                            'gw_items_base_tax_refunded',
                            'gw_items_tax_refunded',
                            'gw_card_base_tax_refunded',
                            'gw_card_tax_refunded',
                            'reward_points_balance',
                            'base_reward_currency_amount',
                            'reward_currency_amount',
                            'base_rwrd_crrncy_amt_invoiced',
                            'rwrd_currency_amount_invoiced',
                            'base_rwrd_crrncy_amnt_refnded',
                            'rwrd_crrncy_amnt_refunded',
                            'reward_points_balance_refund'
                        )
                    );
                }
            }catch(\Exception $exception) {
                $success = FALSE;
                $this->getServiceLocator()->get('logService')
                    ->log(LogService::LEVEL_DEBUG,
                        'mg2_db_init_fail',
                        'DB API init failed - '.$exception->getMessage(),
                        array('hostname'=>$hostname, 'schema'=>$schema, 'message'=>$exception->getMessage()),
                        array('node'=>$magento2Node->getNodeId(), 'exception'=>$exception)
                    );
            }
        }

        return $success;
    }

    /**
     * @param $sql
     */
    protected function debugSql($sql)
    {
        if ($this->debug) {
            $this->getServiceLocator()->get('logService')
                ->log(LogService::LEVEL_DEBUGEXTRA,
                    'mg2_db_sql',
                    'DB API SQL: '.$sql,
                    array('sql'=>$sql),
                    array('node'=>$this->node->getNodeId())
                );
        }
    }

    /**
     * @param Select $select
     * @return array
     */
    protected function getOrdersFromDatabase(Select $select)
    {
        if ($this->orderColumns) {
            $select->columns($this->orderColumns);
        }
        $results = $this->getTableGateway('sales_order')->selectWith($select);

        $data = array();
        foreach ($results as $row) {
            $data[$row['entity_id']] = $row;
        }

        return $data;
    }

    /**
     * Retrieve one magento2 order by increment id
     * @param string $orderIncrementId
     * @return array
     */
    public function getOrderByIncrementId($orderIncrementId)
    {
        $select = new Select('sales_order');
        $select->where(array('increment_id'=>array($orderIncrementId)));

        $data = $this->getOrdersFromDatabase($select);
        if (count($data)) {
            $data = array_shift($data);
        }else{
            $data = NULL;
        }

        return $data;
    }

    /**
     * Retrieve some or all magento2 orders, optionally filtering by an updated at date.
     * @param int|FALSE $storeId
     * @param string|FALSE $updatedSince
     * @param string|FALSE $updatedTo
     * @param array $orderIds
     * @return array
     */
    public function getOrders($storeId = FALSE, $updatedSince = FALSE, $updatedTo = FALSE, array $orderIds = array())
    {
        $select = new Select('sales_order');
        $where = new Where();

        if ($storeId !== FALSE) {
            $where->equalTo('store_id', $storeId);
        }
        if ($updatedSince) {
            $where->greaterThanOrEqualTo('updated_at', $updatedSince);
        }
        if ($updatedTo) {
            $where->lessThanOrEqualTo('updated_at', $updatedTo);
        }
        if (count($orderIds) > 0) {
            $where->in('entity_id', $orderIds);
        }

        $select->where($where);
        $ordersDataArray = $this->getOrdersFromDatabase($select);

        return $ordersDataArray;
    }

    /**
     * Fetch stock levels for all or some products
     * @param array|FALSE $productIds An array of product entity IDs, or FALSE if desiring all.
     * @return array
     */
    public function getStock($productIds = FALSE)
    {
        $criteria = array('stock_id'=>1);
        if (is_array($productIds)) {
            $criteria['product_id'] = $productIds;
        }
        $stockItems = $this->getTableGateway('cataloginventory_stock_item')->select($criteria);

        $stockPerProduct = array();
        foreach ($stockItems as $row) {
            $stockPerProduct[$row['product_id']] = $row['qty'];
        }

        return $stockPerProduct;
    }

    /**
     * Update stock level for a single product
     * @param int $productId Product Entity ID
     * @param float $qty Quantity available
     * @param bool $isInStock Whether the product should be in stock
     */
    public function updateStock($productId, $qty, $isInStock)
    {
        $inventoryTable = $this->getTableGateway('cataloginventory_stock_item');
        $where = array('product_id'=>$productId, 'stock_id'=>1);

        $affectedRows = $inventoryTable->update(array('qty'=>$qty, 'is_in_stock'=>$isInStock), $where);
        if ($affectedRows !== 1) {
            $result = $inventoryTable->select($where);
            foreach ($result as $row) {
                if ($row['qty'] == $qty) {
                    $affectedRows = 1;
                }
                break;
            }
        }

        $logData = array(
            'product_id'=>$productId,
            'qty'=>$qty,
            'is_in_stock'=>$isInStock,
            'affected rows'=>$affectedRows
        );
        if ($affectedRows !== 1) {
            $this->getServiceLocator()->get('logService')->log(LogService::LEVEL_ERROR, 'mg2_db_upd_err_si',
                'Update error on stock with product id '.$productId.'.', $logData);
        }else{
            $this->getServiceLocator()->get('logService')->log(LogService::LEVEL_DEBUG, 'mg2_db_upd_si',
                'Update of stock with product id '.$productId.' was successful.', $logData);
        }

        return ($affectedRows > 0);
    }

    /**
     * Returns whether or not the given customer is subscribed to the newsletter in Magento2 (unconfirmed or confirmed)
     * @param int $customerId The Magento2 customer ID to look up the status for
     * @return bool
     */
    public function getNewsletterStatus($customerId)
    {
        $subscribed = FALSE;
        // TECHNICAL DEBT // ToDo: Implement proper use of Zend functionality
        $sql = "SELECT subscriber_id FROM newsletter_subscriber WHERE customer_id = ".$customerId
            ." AND subscriber_status IN (1, 4)";
        $this->debugSql($sql);

        $newsletterSubscribers = $this->adapter->query($sql, Adapter::QUERY_MODE_EXECUTE);
        foreach ($newsletterSubscribers as $row) {
            if ($row['subscriber_id']) {
                $subscribed = TRUE;
                break;
            }
        }

        return $subscribed;
    }

    /**
     * Get a list of entity IDs that have changed since the given timestamp. Relies on updated_at being set correctly.
     * @param string $entityType
     * @param string $changedSince A date in the MySQL date format (i.e. 2014-01-01 01:01:01)
     * @return array
     */
    public function getChangedEntityIds($entityType, $changedSince)
    {
        // TECHNICAL DEBT // ToDo: Implement proper use of Zend functionality
        $sql = "SELECT entity_id FROM ".$this->getEntityPrefix($entityType)."_entity"
            ." WHERE updated_at >= '".$changedSince."';";

        $this->debugSql($sql);
        $localEntityIds = array();

        $result = $this->adapter->query($sql, Adapter::QUERY_MODE_EXECUTE);
        foreach ($result as $tableRow) {
            $localEntityIds[] = intval($tableRow['entity_id']);
        }

        return $localEntityIds;
    }

    /**
     * @param string $entityType
     * @param string $uniqueId
     * @return int $localEntityId
     */
    public function getLocalId($entityType, $uniqueId)
    {
        if ($entityType == 'product' || $entityType == 'stockitem') {
            $productType = 'product';

            $table = $this->getEntityPrefix($entityType).'_entity';
            $tableGateway = new TableGateway($table, $this->adapter);
            $sql = $tableGateway->getSql();

            $where = new Where();
            $where->equalTo('sku', $uniqueId);
            $sqlSelect = $sql->select()->where($where);
            $selectResult = $tableGateway->selectWith($sqlSelect);

            $sqlString = $sql->getSqlStringForSqlObject($sqlSelect);
            $message = 'Selected entity row from '.$table.' table.';
            $this->getServiceLocator()->get('logService')->log(LogService::LEVEL_INFO,
                    'mg2_db_loid', $message, array('query'=>$sqlString, 'result'=>$selectResult));

            $localEntityId = $selectResult['entity_id'];
        }else{
            $this->getServiceLocator()->get('logService')->log(LogService::LEVEL_WARN,
                'mg2_db_loid_err', 'Invalid entity type: '.$entityType.'.', array());
            $localEntityId = NULL;
        }

        return $localEntityId;
    }

    /**
     * Update an entity in the Magento2 EAV system
     * TECHNICAL DEBT // ToDo: Untested on multi-select / option type attributes.
     * @param string $entityType
     * @param int $entityId
     * @param int $storeId
     * @param array $data Key->value data to update, key is attribute ID.
     * @throws \Exception
     */
    public function updateEntityEav($entityType, $entityId, $storeId, $data)
    {
        $this->adapter->getDriver()->getConnection()->beginTransaction();
        try{
            $staticUpdate = array();
            $attributes = array_keys($data);

            $entityTypeData = $this->getEntityType($entityType);
            $prefix = $this->getEntityPrefix($entityType);

            $attributesByType = $this->preprocessEavAttributes($entityType, $attributes);
            if (isset($attributesByType['static'])) {
                foreach ($attributesByType['static'] as $code) {
                    $staticUpdate[$code] = $data[$code];
                }
                unset($attributesByType['static']);
            }

            if (count($staticUpdate)) {
                $this->getTableGateway($prefix.'_entity')->update($staticUpdate, array('entity_id'=>$entityId));
            }

            $attributesById = array();
            foreach ($attributes as $code) {
                $attribute = $this->getAttribute($entityType, $code);
                $attributesById[$attribute['attribute_id']] = $attribute;
            }

            $affectedRows = 0;
            foreach ($attributesByType as $type=>$singleTypeAttributes) {

                $doSourceTranslation = FALSE;
                $sourceTranslation = array();
                if ($type == 'source_int') {
                    $type = $prefix.'_entity_int';
                    $doSourceTranslation = TRUE;

                    foreach ($singleTypeAttributes as $code=>$attributeId) {
                        $sourceTranslation[$attributeId] =
                            array_flip($this->loadAttributeOptions($attributeId, $storeId));
                    }
                }

                foreach ($singleTypeAttributes as $code=>$attributeId) {

                    $value = $data[$code];
                    if ($doSourceTranslation) {
                        if (isset($sourceTranslation[$attributeId][$value])) {
                            $value = $sourceTranslation[$attributeId][$value];
                        }else{
                            $logMessage = 'DB API found unmatched value '.$value
                                .' for attribute '.$attributesById[$attributeId]['attribute_code'];
                            $this->getServiceLocator()->get('logService')
                                ->log(LogService::LEVEL_WARN,
                                    'mg2_db_upd_invld',
                                    $logMessage,
                                    array('value'=>$value, 'options'=>$sourceTranslation[$attributeId]),
                                    array()
                                );
                        }
                    }

                    $where = $whereForStore0 = array(
                        'entity_id'=>$entityId,
                        'entity_type_id'=>$entityTypeData['entity_type_id'],
                        'store_id'=>$storeId,
                        'attribute_id'=>$attributeId
                    );
                    $whereForStore0['store_id'] = 0;

                    $updateSet = array('value'=>$value);
                    $insertSet = array_merge($where, $updateSet);
                    $insertForStore0 = array_merge($whereForStore0, $updateSet);

                    if ($storeId > 0) {
                        try {
                            $resultsDefault = $this->getTableGateway($type)->select($whereForStore0);
                        }catch (\Exception $exception) {
                            throw new MagelinkException('On updateEntityEav() select: '.$exception->getMessage());
                            $logCode = 'mg2_db_slct_err';
                            $logData = array(
                                'where'=>$whereForStore0,
                                'exception'=>$exception->getMessage()
                            );
//                            $this->getServiceLocator()->get('logService')
//                                ->log(LogService::LEVEL_ERROR, $logCode, $logMessage, $logData);
                        }

                        if (!$resultsDefault || !count($resultsDefault)) {
                            $logCode = 'mg2_db_inst0';
                            $logData = array('insert set 0'=>json_encode($insertForStore0));
                            try{
                                $affectedRows += $this->getTableGateway($type)->insert($insertForStore0);
                                $logLevel = LogService::LEVEL_INFO;
                            }catch (\Exception $exception) {
                                throw new MagelinkException('On updateEntityEav() insert0: '.$exception->getMessage());
                                $logLevel = LogService::LEVEL_ERROR;
                                $logCode .= '_err';
                                $logData['exception'] = $exception->getMessage();
                            }
//                            $this->getServiceLocator()->get('logService')
//                                ->log(LogService::LEVEL_INFO, 'mg2_db_insert0', $logMessage, $logData);
                        }
                    }

                    $resultsStore = $this->getTableGateway($type)->select($where);
                    if (!$resultsStore || !count($resultsStore)) {
                        $logCode = 'mg2_db_inst';
                        $logData = array('insert set'=>json_encode($insertSet));
                        try {
                            $affectedRows += $this->getTableGateway($type)->insert($insertSet);
                            $logLevel = LogService::LEVEL_INFO;
                        }catch (\Exception $exception) {
                            throw new MagelinkException('On updateEntityEav() insert: '.$exception->getMessage());
                            $logLevel = LogService::LEVEL_ERROR;
                            $logCode .= '_err';
                            $logData['exception'] = $exception->getMessage();
                        }
//                        $this->getServiceLocator()->get('logService')
//                            ->log(LogService::LEVEL_INFO, $logCode, $logMessage, $logData);
                    }else{
                        $logCode = 'mg2_db_upd';
                        $logData = array('update set'=>json_encode($updateSet), 'where'=>$where);
                        try {
                            $affectedRows += $this->getTableGateway($type)->update($updateSet, $where);
                            $logLevel = LogService::LEVEL_INFO;
                        }catch (\Exception $exception) {
                            throw new MagelinkException('On updateEntityEav() select: '.$exception->getMessage());
                            $logLevel = LogService::LEVEL_ERROR;
                            $logCode .= '_err';
                            $logData['exception'] = $exception->getMessage();
                        }
//                        $this->getServiceLocator()->get('logService')
//                            ->log(LogService::LEVEL_INFO, $logCode, $logMessage, $logData);
                    }
                }
            }

            $this->adapter->getDriver()->getConnection()->commit();

        }catch(\Exception $exception) {
            $this->adapter->getDriver()->getConnection()->rollback();
            throw $exception;
            $affectedRows = 0;
        }

        return ($affectedRows > 0);
//        return $affectedRows;
    }

    /**
     * Load entities from the EAV tables, with the specified attributes
     * @param string $entityType
     * @param array|NULL $entityIds Entity IDs to fetch, or NULL if load all
     * @param int|FALSE $storeId
     * @param array $attributes
     * @return array
     * @throws MagelinkException
     */
    public function loadEntitiesEav($entityType, $entityIds, $storeId, $attributes)
    {
        $entityTypeData = $this->getEntityType($entityType);
        $prefix = $this->getEntityPrefix($entityType);

        if ($entityIds != NULL) {
            $entityRowRaw = $this->getTableGateway($prefix.'_entity')
                ->select(array('entity_id'=>$entityIds));
        }else{
            $entityRowRaw = $this->getTableGateway($prefix.'_entity')
                ->select();
        }
        if (!$entityRowRaw || !count($entityRowRaw)) {
            return array();
        }

        $populateEntityIds = FALSE;
        if ($entityIds == NULL) {
            $entityIds = array();
            $populateEntityIds = TRUE;
        }
        $entityRow = array();
        foreach ($entityRowRaw as $row) {
            $entityRow[$row['entity_id']] = $row;
            if ($populateEntityIds) {
                $entityIds[] = $row['entity_id'];
            }
        }

        $attributesByType = $this->preprocessEavAttributes($entityType, $attributes);

        $attributesById = array();
        foreach ($attributes as $code) {
            $attribute = $this->getAttribute($entityType, $code);
            $attributesById[$attribute['attribute_id']] = $attribute;
        }

        $results = array();
        foreach ($entityIds as $id) {
            $results[$id] = array('entity_id'=>$id);
        }

        foreach ($attributesByType as $type=>$typeAttributes) {
            if ($type == 'static') {
                foreach ($typeAttributes as $code=>$attributeId) {
                    foreach ($entityIds as $entityId) {
                        if (isset($entityRow[$entityId])) {
                            if (isset($entityRow[$entityId][$code])) {
                                $results[$entityId][$code] = $entityRow[$entityId][$code];
                            }else{
                                $message = 'Invalid static attribute '.$code.' on entity with id '.$entityId
                                    .' (type '.$entityType.', store '.$storeId.').';
                                throw new MagelinkException($message);
                            }
                        }
                    }
                }
            }else{
                $doSourceTranslation = FALSE;
                $sourceTranslation = array();
                if ($type == 'source_int') {
                    $type = $prefix.'_entity_int';
                    $doSourceTranslation = TRUE;

                    foreach ($typeAttributes as $code=>$attributeId) {
                        $sourceTranslation[$attributeId] = $this->loadAttributeOptions($attributeId, $storeId);
                    }
                }

                $where = $whereForStore0 = array(
                    'entity_id'=>$entityIds,
                    'store_id'=>$storeId,
                    'attribute_id'=>array_values($typeAttributes),
                );
                $whereForStore0['store_id'] = 0;

                $resultsDefault = $this->getTableGateway($type)->select($whereForStore0);
                if ($storeId !== FALSE) {
                    $resultsStore = $this->getTableGateway($type)->select($where);
                }else{
                    $resultsStore = array();
                }

                foreach ($resultsDefault as $row) {
                    $value = $row['value'];
                    if ($doSourceTranslation) {
                        if (isset($sourceTranslation[$row['attribute_id']][$value])) {
                            $value = $sourceTranslation[$row['attribute_id']][$value];
                        }else{
                            $logMessage = 'DB API found unmatched value '.$value.' for attribute '
                                .$attributesById[$row['attribute_id']]['attribute_code'];
                            $logData = array('row'=>$row, 'options'=>$sourceTranslation[$row['attribute_id']]);
                            $this->getServiceLocator()->get('logService')
                                ->log(LogService::LEVEL_WARN, 'mg2_db_ivld', $logMessage, $logData);
                        }
                    }
                    $results[intval($row['entity_id'])][$attributesById[$row['attribute_id']]['attribute_code']] = $value;
                }

                if ($storeId !== FALSE) {
                    foreach ($resultsStore as $row) {
                        $value = $row['value'];
                        $entityId = intval($row['entity_id']);
                        if ($doSourceTranslation) {
                            if (isset($sourceTranslation[$row['attribute_id']][$value])) {
                                $value = $sourceTranslation[$row['attribute_id']][$value];
                            }else{
                                $logMessage = 'DB API found unmatched value '.$value.' for att '
                                    .$attributesById[$row['attribute_id']]['attribute_code'];
                                $logData = array('row'=>$row, 'options'=>$sourceTranslation[$row['attribute_id']]);
                                $this->getServiceLocator()->get('logService')
                                    ->log(LogService::LEVEL_WARN, 'mg2_db_ivld_stor', $logMessage, $logData);
                            }
                        }

                        $results[$entityId][$attributesById[$row['attribute_id']]['attribute_code']] = $value;
                    }
                }
            }
        }

        return $results;
    }

    /**
     * Returns a key-value array of option id -> value for the given attribute
     * @param int $attributeId
     * @param int $storeId
     * @return array
     */
    protected function loadAttributeOptions($attributeId, $storeId = 0)
    {
        $optionIds = array();
        $attributeOptions = array();

        $options = $this->getTableGateway('eav_attribute_option')->select(array('attribute_id'=>$attributeId));
        foreach ($options as $row) {
            $optionIds[] = $row['option_id'];
        }

        $values = $this->getTableGateway('eav_attribute_option_value')
            ->select(array('option_id'=>$optionIds, 'store_id'=>array(0, $storeId)));
        foreach ($values as $row) {
            $addRow = $row['store_id'] > 0 ||
                $row['store_id'] == 0 && !isset($attributeOptions[$row['option_id']]);
            if ($addRow) {
                $attributeOptions[$row['option_id']] = $row['value'];
            }
        }

        return $attributeOptions;
    }

    /**
     * @return bool $successful
     */
    public function correctPricesOnDefault($localId, $prices)
    {
        $logCode = 'mg2_db_mv_prc';
        $attributesByTable = array(
            'catalog_product_entity_datetime'=>array('special_from_date'=>76, 'special_from_date'=>77),
            'catalog_product_entity_decimal'=>array('price'=>74, 'special_price'=>75, 'msrp'=>117)
        );

        $where = new Where();
        $where->equalTo('store_id', 0);
        $where->and->equalTo('entity_id', $localId);

        $updatedRows = 0;
        $sqlQueries = array();

        foreach ($attributesByTable as $table=>$attributeIdsByCode) {
            foreach ($attributeIdsByCode as $code=>$attributeId) {
                $attributeWhere = clone $where;
                $attributeWhere->and->equalTo('attribute_id', $attributeId);

                try{
                    $tableGateway = new TableGateway($table, $this->adapter);
                    $sql = $tableGateway->getSql();
                    $sqlUpdate = $sql->update()->set(array('value'=>$prices[$code]))->where($where);
                    $updatedRows += $tableGateway->updateWith($sqlUpdate);
                    $sqlQueries[] = $sql->getSqlStringForSqlObject($sqlUpdate);
                }catch(\Exception $exception){
                    $this->getServiceLocator()->get('logService')->log(
                        LogService::LEVEL_DEBUG,
                        $logCode.'err',
                        'Error on updating default store data: '.$exception->getMessage(),
                        array('table'=>$table, 'updated rows'=>$updatedRows, 'queries'=>$sqlQueries)
                    );
                }
            }
        }

        $this->getServiceLocator()->get('logService')->log(LogService::LEVEL_DEBUG, $logCode,
            ($updatedRows > 0 ? 'Updated default store data' : 'No default store data was updated'),
            array('local id'=>$localId, 'prices'=>$prices, 'attributesByTable'=>$attributesByTable,
                'updated rows'=>$updatedRows, 'queries'=>$sqlQueries));

        return (bool) $updatedRows;
    }

    /**
     * @param int $localId
     * @param int $storeId
     * @return bool $successful
     */
    public function removeAllStoreSpecificInformationOnProducts($localId, $storeId)
    {
        $logCode = 'mg2_db_rm_spc';
        $mainTable = 'catalog_product_entity';
        $eavTableTypes = array('datetime', 'decimal', 'int', 'text', 'tier_price', 'varchar');

        $where = new Where();
        $where->greaterThan('store_id', 0);
        $where->and->equalTo('entity_id', $localId);

        $deletedRows = 0;
        $sqlQueries = array();

        foreach ($eavTableTypes as $prefix) {
            $table = $mainTable.'_'.$prefix;

            try{
                $tableGateway = new TableGateway($table, $this->adapter);
                $sql = $tableGateway->getSql();
                $sqlDelete = $sql->delete()->where($where);
                $deletedRows += $tableGateway->deleteWith($sqlDelete);
                $sqlQueries[] = $sql->getSqlStringForSqlObject($sqlDelete);
            }catch (\Exception $exception) {
                $this->getServiceLocator()->get('logService')->log(LogService::LEVEL_DEBUG,
                    $logCode.'err',
                    'Error on deleting store specific data: '.$exception->getMessage(),
                    array('table'=>$table, 'deleted rows'=>$deletedRows, 'queries'=>$sqlQueries)
                );
            }
        }

        $this->getServiceLocator()->get('logService')->log(LogService::LEVEL_DEBUG, $logCode,
            ($deletedRows > 0 ? 'Removed store specific data' : 'No store specific data was removed'),
            array('local id'=>$localId, 'deleted rows'=>$deletedRows, 'queries'=>$sqlQueries)
        );

        return (bool) $deletedRows;
    }

    /**
     * Preprocess a list of attribute codes into the respective tables
     * @param string $entityType
     * @param array $attributes
     * @throws MagelinkException if an invalid attribute code is passed
     * @return array $attributesByType
     */
    protected function preprocessEavAttributes($entityType, $attributes)
    {
        // ToDo : Cleanup this method
        $prefix = $this->getEntityPrefix($entityType);

        if (!isset($attributesByType['static'])) {
            $attributesByType['static'] = array();
        }

        $attributesByType = array();
        foreach ($attributes as $code) {
            if (in_array($code, array('attribute_set_id', 'type_id'))) {
                $attributesByType['static'][$code] = $code;
                continue;
            }

            $code = trim($code);
            if (!strlen($code)) {
                continue;
            }

            $attribute = $this->getAttribute($entityType, $code);
            if ($attribute == NULL) {
                // TECHNICAL DEBT // ToDo: throw new MagelinkException('Invalid Magento2 attribute code ' . $code . ' for ' . $entityType);
            }else{
                $table = $this->getAttributeTable($prefix, $attribute);

                if (!isset($attributesByType[$table])) {
                    $attributesByType[$table] = array();
                }

                $attributesByType[$table][$code] = $attribute['attribute_id'];
            }
        }

        return $attributesByType;
    }

    /**
     * Get the table used for storing a particular attribute, or "static" if it exists in the entity table.
     * @param string $prefix The table prefix to be used, e.g. "catalog_product".
     * @param array $attrData
     * @return string The table name or "static"
     */
    protected function getAttributeTable($prefix, $attributeData)
    {
        if ($attributeData['backend_type'] == 'static') {
            return 'static';

        }elseif ($attributeData['backend_table'] != NULL) {
            return $attributeData['backend_table'];

        }elseif ($attributeData['backend_type'] == 'int' && $attributeData['source_model'] == 'eav/entity_attribute_source_table') {
            return 'source_int';

        }else{
            return $prefix.'_entity_' . $attributeData['backend_type'];
        }
    }

    /**
     * Returns the table prefix for entities of the given type
     * @param $entityType
     * @return string
     * @throws \Magelink\Exception\MagelinkException
     */
    protected function getEntityPrefix($entityType)
    {
        switch ($entityType) {
            case 'catalog_product':
            case 'catalog_category':
            case 'customer':
            case 'customer_address':
                return $entityType;
            case 'rma_item':
                return 'enterprise_rma_item';
            default:
                // TECHNICAL DEBT // ToDo: Check : Maybe warn? This should be a safe default
                return $entityType;
        }
    }
    /**
     * Returns the entity type table entry for the given type
     * @param $entityTypeCode
     * @return NULL
     */
    protected function getEntityType($entityTypeCode)
    {
        if (!isset($this->entityTypes[$entityTypeCode])) {
            $this->entityTypes[$entityTypeCode] = NULL;
            $response = $this->getTableGateway('eav_entity_type')->select(array('entity_type_code'=>$entityTypeCode));

            foreach ($response as $row) {
                $this->entityTypes[$entityTypeCode] = $row;
                break;
            }
        }

        return $this->entityTypes[$entityTypeCode];
    }

    /**
     * Returns the eav attribute table entry for the given code
     * @param $entityType
     * @param $attributeCode
     * @return NULL
     */
    protected function getAttribute($entityType, $attributeCode)
    {
        $entityType = $this->getEntityType($entityType);
        $entityType = $entityType['entity_type_id'];

        if (!isset($this->attributesByEntityType[$entityType])) {
            $this->attributesByEntityType[$entityType] = array();
        }

        if (!isset($this->attributesByEntityType[$entityType][$attributeCode])) {
            $this->attributesByEntityType[$entityType][$attributeCode] = NULL;

            try{
                $response = $this->getTableGateway('eav_attribute')
                    ->select(array('entity_type_id'=>$entityType, 'attribute_code'=>$attributeCode));
            }catch (\Exception $exception) {
                throw new MagelinkException('On getAttribute(): '.$exception->getMessage());
            }

            foreach ($response as $row) {
                $this->attributesByEntityType[$entityType][$attributeCode] = $row;
                break;
            }
        }

        return $this->attributesByEntityType[$entityType][$attributeCode];
    }

    /**
     * Returns a new TableGateway instance for the requested table
     * @param string $table
     * @return \Zend\Db\TableGateway\TableGateway
     */
    protected function getTableGateway($table)
    {
        if (!isset($this->tableGateways[$table])) {
            $this->tableGateways[$table] = new TableGateway($table, $this->adapter);
        }

        return $this->tableGateways[$table];
    }

}
