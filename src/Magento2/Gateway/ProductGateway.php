<?php
/**
 * Magento2\Gateway\OrderGateway
 * @category Magento2
 * @package Magento2\Gateway
 * @author Matt Johnston
 * @author Andreas Gerhards <andreas@lero9.co.nz>
 * @copyright Copyright(c) 2014 LERO9 Ltd.
 * @license Commercial - All Rights Reserved
 */

namespace Magento2\Gateway;

use Entity\Update;
use Entity\Action;
use Magento2\Service\Magento2Service;
use Log\Service\LogService;
use Magelink\Exception\MagelinkException;
use Magelink\Exception\SyncException;
use Magelink\Exception\NodeException;
use Magelink\Exception\GatewayException;
use Node\Entity;


class ProductGateway extends AbstractGateway
{
    const GATEWAY_ENTITY = 'product';
    const GATEWAY_ENTITY_CODE = 'p';

    /**
     * Initialize the gateway and perform any setup actions required.
     * @param string $entityType
     * @return bool $success
     * @throws GatewayException
     */
    protected function _init($entityType)
    {
        $success = parent::_init($entityType);

        if ($entityType != 'product') {
            throw new GatewayException('Invalid entity type for this gateway');
            $success = FALSE;
        }else{
            try {
                $attributeSets = $this->restV1->get('eav/attribute-sets/list',array());
            }catch (\Exception $exception) {
                throw new GatewayException($exception->getMessage(), $exception->getCode(), $exception);
                $success = FALSE;
            }

            $this->_attributeSets = array();
            foreach ($attributeSets as $attributeSetArray) {
                $this->_attributeSets[$attributeSetArray['set_id']] = $attributeSetArray;
            }

            $this->getServiceLocator()->get('logService')->log(LogService::LEVEL_DEBUG, $this->getLogCode().'_init',
                'Initialised Magento2 product gateway.',
                array('db api'=>(bool) $this->db, 'rest api'=>(bool) $this->restV1,
                    'retrieved attributes'=>$attributeSets, 'stored attributes'=>$this->_attributeSets)
            );
        }

        return $success;
    }

    /**
     * Retrieve and action all updated records(either from polling, pushed data, or other sources).
     * @throws MagelinkException
     * @throws NodeException
     * @throws SyncException
     * @throws GatewayException
     */
    public function retrieveEntities()
    {
        $this->getServiceLocator()->get('logService')
            ->log(LogService::LEVEL_INFO,
                $this->getLogCode().'_re_time',
                'Retrieving products updated since '.$this->lastRetrieveDate,
               array('type'=>'product', 'timestamp'=>$this->lastRetrieveDate)
            );

        $additional = $this->_node->getConfig('product_attributes');
        if (is_string($additional)) {
            $additional = explode(',', $additional);
        }
        if (!$additional || !is_array($additional)) {
            $additional = array();
        }

        if ($this->db) {
            $api = 'db';
            try {
                $updatedProducts = $results = $this->db->getChangedEntityIds('catalog_product', $this->lastRetrieveDate);
            }catch (\Exception $exception) {
                throw new GatewayException($exception->getMessage(), $exception->getCode(), $exception);
            }

            if (count($updatedProducts)) {
                $attributes = array(
                    'sku',
                    'name',
                    'attribute_set_id',
                    'type_id',
                    'description',
                    'short_description',
                    'status',
                    'visibility',
                    'price',
                    'tax_class_id',
                    'special_price',
                    'special_from_date',
                    'special_to_date'
                );

                foreach ($additional as $key=>$attributeCode) {
                    if (!strlen(trim($attributeCode))) {
                        unset($additional[$key]);
                    }elseif (!$this->entityConfigService->checkAttribute('product', $attributeCode)) {
                        $this->entityConfigService->createAttribute(
                            $attributeCode,
                            $attributeCode,
                            FALSE,
                            'varchar',
                            'product',
                            'Magento2 Additional Attribute'
                        );
                        try{
                            $this->_nodeService->subscribeAttribute(
                                $this->_node->getNodeId(),
                                $attributeCode,
                                'product',
                                TRUE
                            );
                        }catch(\Exception $exception) {
                            throw new GatewayException($exception->getMessage(), $exception->getCode(), $exception);
                        }
                    }
                }
                $attributes = array_merge($attributes, $additional);

                foreach ($updatedProducts as $localId) {
                    $sku = NULL;
                    $combinedData = array();
                    $storeIds = array_keys($this->_node->getStoreViews());

                    foreach ($storeIds as $storeId) {
                        if ($storeId == 0) {
                            $storeId = FALSE;
                        }

                        $brands = FALSE;
                        if (in_array('brand', $attributes)) {
                            try{
                                $brands = $this->db->loadEntitiesEav('brand', NULL, $storeId, array('name'));
                                if (!is_array($brands) || count($brands) == 0) {
                                    $this->getServiceLocator()->get('logService')->log(LogService::LEVEL_WARN,
                                        $this->getLogCode().'_db_nobrnds',
                                        'Something is wrong with the brands retrieval.',
                                        array('brands'=>$brands)
                                    );
                                    $brands = FALSE;
                                }
                            }catch( \Exception $exception ){
                                $brands = FALSE;
                            }
                        }

                        try{
                            $productsData = $this->db
                                ->loadEntitiesEav('catalog_product', array($localId), $storeId, $attributes);
                            $this->getServiceLocator()->get('logService')->log(LogService::LEVEL_DEBUGEXTRA,
                                $this->getLogCode().'_db_data', 'Loaded product data from Magento2 via DB api.',
                                array('local id'=>$localId, 'store id'=>$storeId, 'data'=>$productsData)
                            );
                        }catch(\Exception $exception) {
                            throw new GatewayException($exception->getMessage(), $exception->getCode(), $exception);
                        }

                        foreach ($productsData as $productId=>$rawData) {
                            // @todo: Combine this two methods into one
                            $productData = $this->convertFromMagento($rawData, $additional);
                            $productData = $this->getServiceLocator()->get('magento2Service')
                                ->mapProductData($productData, $storeId);

                            if (is_array($brands) && isset($rawData['brand']) && is_numeric($rawData['brand'])) {
                                if (isset($brands[intval($rawData['brand'])])) {
                                    $productData['brand'] = $brands[intval($rawData['brand'])]['name'];
                                }else{
                                    $productData['brand'] = NULL;
                                    $this->getServiceLocator()->get('logService')->log(LogService::LEVEL_WARN,
                                        $this->getLogCode().'_db_nomabra',
                                        'Could not find matching brand for product '.$sku.'.',
                                        array('brand (key)'=>$rawData['brand'], 'brands'=>$brands)
                                    );
                                }
                            }

                            if (isset($rawData['attribute_set_id'])
                                    && isset($this->_attributeSets[intval($rawData['attribute_set_id'])])) {
                                $productData['product_class'] = $this->_attributeSets[intval(
                                    $rawData['attribute_set_id']
                                )]['name'];
                            }else{
                                $this->getServiceLocator()->get('logService')->log(LogService::LEVEL_WARN,
                                    $this->getLogCode().'_db_noset',
                                    'Issue with attribute set id on product '.$sku.'. Check $rawData[attribute_set_id].',
                                    array('raw data'=>$rawData)
                                );
                            }
                        }

                        if (count($combinedData) == 0) {
                            $sku = $rawData['sku'];
                            $combinedData = $productData;
                        }else {
                            $combinedData = array_replace_recursive($combinedData, $productData, $combinedData);
                        }
                    }

                    $this->getServiceLocator()->get('logService')
                        ->log(LogService::LEVEL_DEBUGEXTRA, $this->getLogCode().'_db_comb',
                            'Combined data for Magento2 product id '.$localId.'.',
                            array('combined data'=>$combinedData)
                        );

                    $parentId = NULL; // @todo: Calculate

                    try{
                        $this->processUpdate($productId, $sku, $storeId, $parentId, $combinedData);
                    }catch( \Exception $exception ){
                        // store as sync issue
                        throw new GatewayException($exception->getMessage(), $exception->getCode(), $exception);
                    }
                }
            }
        }elseif ($this->restV1) {
            $api = 'restV1';
            // @todo : Multistore capability!
            $storeId = NULL;
            try {
                $results = $this->restV1->get('product', array(
                    'filter'=>array(array(
                        'field'=>'updated_at',
                        'value'=>$this->lastRetrieveDate,
                        'condition_type'=>'gt'
                    ))
                ));
            }catch (\Exception $exception) {
                throw new GatewayException($exception->getMessage(), $exception->getCode(), $exception);
            }

            foreach ($results as $productData) {
                $productId = $productData['product_id'];
                $sku = $productData['sku'];

                // @todo
                $productData = array_merge(
                    $productData,
                    $this->loadFullProduct($sku, $storeId)
                );

                $this->getServiceLocator()->get('logService')->log(LogService::LEVEL_DEBUGEXTRA,
                    $this->getLogCode().'_rest_data', 'Loaded product data from Magento2 via SOAP api.',
                    array('sku'=>$productData['sku'], 'data'=>$productData)
                );

                if (isset($this->_attributeSets[intval($productData['set']) ])) {
                    $productData['product_class'] = $this->_attributeSets[intval($productData['set']) ]['name'];
                    unset($productData['set']);
                }else{
                    $this->getServiceLocator()->get('logService')
                        ->log(LogService::LEVEL_WARN,
                            $this->getLogCode().'_rest_uset',
                            'Unknown attribute set ID '.$productData['set'],
                           array('set'=>$productData['set'], 'sku'=>$productData['sku'])
                        );
                }

                if (isset($productData[''])) {
                    unset($productData['']);
                }

                unset($productData['category_ids']); // @todo parse into categories
                unset($productData['website_ids']); // Not used

                $productId = $productData['product_id'];
                $parentId = NULL; // @todo: Calculate
                $sku = $productData['sku'];
                unset($productData['product_id']);
                unset($productData['sku']);

                try {
                    $this->processUpdate($productId, $sku, $storeId, $parentId, $productData);
                }catch (\Exception $exception) {
                    // store as sync issue
                    throw new GatewayException($exception->getMessage(), $exception->getCode(), $exception);
                }
            }
        }else{
            throw new NodeException('No valid API available for sync');
            $api = '-';
        }

        $this->_nodeService
            ->setTimestamp($this->_nodeEntity->getNodeId(), 'product', 'retrieve', $this->getNewRetrieveTimestamp());

        $seconds = ceil($this->getAdjustedTimestamp() - $this->getNewRetrieveTimestamp());
        $message = 'Retrieved '.count($results).' products in '.$seconds.'s up to '
            .strftime('%H:%M:%S, %d/%m', $this->retrieveTimestamp).' via '.$api.' api.';
        $logData = array('type'=>'product', 'amount'=>count($results), 'period [s]'=>$seconds);
        if (count($results) > 0) {
            $logData['per entity [s]'] = round($seconds / count($results), 3);
        }
        $this->getServiceLocator()->get('logService')->log(LogService::LEVEL_INFO, $this->getLogCode().'_re_no', $message, $logData);
    }

    /**
     * @param int $productId
     * @param string $sku
     * @param int $storeId
     * @param int $parentId
     * @param array $data
     * @return \Entity\Entity|NULL
     */
    protected function processUpdate($productId, $sku, $storeId, $parentId, array $data)
    {
        /** @var boolean $needsUpdate Whether we need to perform an entity update here */
        $needsUpdate = TRUE;

        $existingEntity = $this->_entityService->loadEntityLocal($this->_node->getNodeId(), 'product', 0, $productId);
        if (!$existingEntity) {
            $existingEntity = $this->_entityService->loadEntity($this->_node->getNodeId(), 'product', 0, $sku);
            $noneOrWrongLocalId = $this->_entityService->getLocalId($this->_node->getNodeId(), $existingEntity);

            if (!$existingEntity) {
                $existingEntity = $this->_entityService
                    ->createEntity($this->_node->getNodeId(), 'product', 0, $sku, $data, $parentId);
                $this->_entityService->linkEntity($this->_node->getNodeId(), $existingEntity, $productId);
                $this->getServiceLocator()->get('logService')
                    ->log(LogService::LEVEL_INFO,
                        $this->getLogCode().'_new',
                        'New product '.$sku,
                       array('sku'=>$sku),
                       array('node'=>$this->_node, 'entity'=>$existingEntity)
                    );
                try{
                    $stockEntity = $this->_entityService
                        ->createEntity($this->_node->getNodeId(), 'stockitem', 0, $sku, array(), $existingEntity);
                    $this->_entityService->linkEntity($this->_node->getNodeId(), $stockEntity, $productId);
                }catch (\Exception $exception) {
                    $this->getServiceLocator() ->get('logService')
                        ->log(LogService::LEVEL_WARN,
                            $this->getLogCode().'_si_ex',
                            'Already existing stockitem for new product '.$sku,
                           array('sku'=>$sku),
                           array('node'=>$this->_node, 'entity'=>$existingEntity)
                        );
                }
                $needsUpdate = FALSE;
            }elseif ($noneOrWrongLocalId != NULL) {
                $this->_entityService->unlinkEntity($this->_node->getNodeId(), $existingEntity);
                $this->_entityService->linkEntity($this->_node->getNodeId(), $existingEntity, $productId);

                $stockEntity = $this->_entityService->loadEntity($this->_node->getNodeId(), 'stockitem', 0, $sku);
                if ($this->_entityService->getLocalId($this->_node->getNodeId(), $stockEntity) != NULL) {
                    $this->_entityService->unlinkEntity($this->_node->getNodeId(), $stockEntity);
                }
                $this->_entityService->linkEntity($this->_node->getNodeId(), $stockEntity, $productId);

                $this->getServiceLocator() ->get('logService')
                    ->log(LogService::LEVEL_ERROR,
                        $this->getLogCode().'_relink',
                        'Incorrectly linked product '.$sku.' ('.$noneOrWrongLocalId.'). Re-linked now.',
                       array('code'=>$sku, 'wrong local id'=>$noneOrWrongLocalId),
                       array('node'=>$this->_node, 'entity'=>$existingEntity)
                    );
            }else{
                $this->getServiceLocator() ->get('logService')
                    ->log(LogService::LEVEL_INFO,
                        $this->getLogCode().'_link',
                        'Unlinked product '.$sku,
                       array('sku'=>$sku),
                       array('node'=>$this->_node, 'entity'=>$existingEntity)
                    );
                $this->_entityService->linkEntity($this->_node->getNodeId(), $existingEntity, $productId);
            }
        }else{
            $this->getServiceLocator()->get('logService')
                ->log(LogService::LEVEL_INFO,
                    $this->getLogCode().'_upd',
                    'Updated product '.$sku,
                   array('sku'=>$sku),
                   array('node'=>$this->_node, 'entity'=>$existingEntity, 'data'=>$data)
                );
        }

        if ($needsUpdate) {
            $this->_entityService->updateEntity($this->_node->getNodeId(), $existingEntity, $data, FALSE);
        }

        return $existingEntity;
    }

    /**
     * Load detailed product data from Magento2
     * @param $productId
     * @param $storeId
     * @param \Entity\Service\EntityConfigService $this->entityConfigService
     * @return array
     * @throws \Magelink\Exception\MagelinkException
     */
    public function loadFullProduct($sku, $storeId) {

        $additional = $this->_node->getConfig('product_attributes');
        if (is_string($additional)) {
            $additional = explode(',', $additional);
        }
        if (!$additional || !is_array($additional)) {
            $additional = array();
        }

        // 'custom_attributes'
        $data = array(
            $storeId,
            array('additional_attributes'=>$additional),
            'id',
        );

        $productInfo = $this->restV1->get('products/'.$sku, $data);

        if (!$productInfo && !$productInfo['sku']) {
            // store as sync issue
            throw new GatewayException('Invalid product info response');
            $data = NULL;
        }else{
            $data = $this->convertFromMagento2($productInfo, $additional);

            foreach ($additional as $attributeCode) {
                $attributeCode = strtolower(trim($attributeCode));

                if (strlen($attributeCode)) {
                    if (!array_key_exists($attributeCode, $data)) {
                        $data[$attributeCode] = NULL;
                    }

                    if (!$this->entityConfigService->checkAttribute('product', $attributeCode)) {
                        $this->entityConfigService->createAttribute(
                            $attributeCode,
                            $attributeCode,
                            0,
                            'varchar',
                            'product',
                            'Custom Magento2 attribute'
                        );

                        try {
                            $this->getServiceLocator()->get('nodeService')->subscribeAttribute(
                                $this->_node->getNodeId(),
                                $attributeCode,
                                'product'
                            );
                        }catch (\Exception $exception) {
                            // Store as sync issue
                            throw new GatewayException($exception->getMessage(), $exception->getCode(), $exception);
                            $data = NULL;
                        }
                    }
                }
            }
        }

        return $data;
    }

    /**
     * Converts Magento2-named attributes into our internal Magelink attributes / formats.
     * @param array $rawData Input array of Magento2 attribute codes
     * @param array $additional Additional product attributes to load in
     * @return array
     */
    protected function convertFromMagento($rawData, $additional) {
        $data = array();
        if (isset($rawData['type_id'])) {
            $data['type'] = $rawData['type_id'];
        }else{
            if (isset($rawData['type'])) {
                $data['type'] = $rawData['type'];
            }else{
                $data['type'] = NULL;
            }
        }
        if (isset($rawData['name'])) {
            $data['name'] = $rawData['name'];
        }else{
            $data['name'] = NULL;
        }
        if (isset($rawData['description'])) {
            $data['description'] = $rawData['description'];
        }else{
            $data['description'] = NULL;
        }
        if (isset($rawData['short_description'])) {
            $data['short_description'] = $rawData['short_description'];
        }else{
            $data['short_description'] = NULL;
        }
        if (isset($rawData['status'])) {
            $data['enabled'] =($rawData['status'] == 1) ? 1 : 0;
        }else{
            $data['enabled'] = 0;
        }
        if (isset($rawData['visibility'])) {
            $data['visible'] =($rawData['visibility'] == 4) ? 1 : 0;
        }else{
            $data['visible'] = 0;
        }
        if (isset($rawData['price'])) {
            $data['price'] = $rawData['price'];
        }else{
            $data['price'] = NULL;
        }
        if (isset($rawData['tax_class_id'])) {
            $data['taxable'] =($rawData['tax_class_id'] == 2) ? 1 : 0;
        }else{
            $data['taxable'] = 0;
        }
        if (isset($rawData['special_price'])) {
            $data['special_price'] = $rawData['special_price'];

            if (isset($rawData['special_from_date'])) {
                $data['special_from_date'] = $rawData['special_from_date'];
            }else{
                $data['special_from_date'] = NULL;
            }
            if (isset($rawData['special_to_date'])) {
                $data['special_to_date'] = $rawData['special_to_date'];
            }else{
                $data['special_to_date'] = NULL;
            }
        }else{
            $data['special_price'] = NULL;
            $data['special_from_date'] = NULL;
            $data['special_to_date'] = NULL;
        }

        if (isset($rawData['additional_attributes'])) {
            foreach ($rawData['additional_attributes'] as $pair) {
                $attributeCode = trim(strtolower($pair['key']));
                if (!in_array($attributeCode, $additional)) {
                    throw new GatewayException('Invalid attribute returned by Magento2: '.$attributeCode);
                }
                if (isset($pair['value'])) {
                    $data[$attributeCode] = $pair['value'];
                }else{
                    $data[$attributeCode] = NULL;
                }
            }
        }else{
            foreach ($additional as $code) {
                if (isset($rawData[$code])) {
                    $data[$code] = $rawData[$code];
                }
            }
        }

        return $data;
    }

    /**
     * Restructure data for rest call and return this array.
     * @param array $data
     * @param array $customAttributes
     * @return array $restData
     * @throws \Magelink\Exception\MagelinkException
     */
    protected function getUpdateDataForSoapCall(array $data, array $customAttributes)
    {
        // Restructure data for rest call
        $restData = array(
            'additional_attributes'=>array(
                'single_data'=>array(),
                'multi_data'=>array()
            )
        );
        $removeSingleData = $removeMultiData = TRUE;

        foreach ($data as $code=>$value) {
            $isCustomAttribute = in_array($code, $customAttributes);
            if ($isCustomAttribute) {
                if (is_array($data[$code])) {
                    // @todo(maybe) : Implement
                    throw new GatewayException("This gateway doesn't support multi_data custom attributes yet.");
                    $removeMultiData = FALSE;
                }else{
                    $restData['additional_attributes']['single_data'][] = array(
                        'key'=>$code,
                        'value'=>$value,
                    );
                    $removeSingleData = FALSE;
                }
            }else{
                $restData[$code] = $value;
            }
        }

        if ($removeSingleData) {
            unset($data['additional_attributes']['single_data']);
        }
        if ($removeMultiData) {
            unset($data['additional_attributes']['multi_data']);
        }
        if ($removeSingleData && $removeMultiData) {
            unset($data['additional_attributes']);
        }

        return $restData;
    }

    /**
     * Write out all the updates to the given entity.
     * @param \Entity\Entity $entity
     * @param string[] $attributes
     * @param int $type
     */
    public function writeUpdates(\Entity\Entity $entity, $attributes, $type = Update::TYPE_UPDATE)
    {
        $nodeId = $this->_node->getNodeId();
        $sku = $entity->getUniqueId();

        $customAttributes = $this->_node->getConfig('product_attributes');
        if (is_string($customAttributes)) {
            $customAttributes = explode(',', $customAttributes);
        }
        if (!$customAttributes || !is_array($customAttributes)) {
            $customAttributes = array();
        }

        $this->getServiceLocator()->get('logService')
            ->log(LogService::LEVEL_DEBUGEXTRA,
                $this->getLogCode().'_wrupd',
                'Attributes for update of product '.$sku.': '.var_export($attributes, TRUE),
               array('attributes'=>$attributes, 'custom'=>$customAttributes),
               array('entity'=>$entity)
            );

        $originalData = $entity->getFullArrayCopy();
        $attributeCodes = array_unique(array_merge(
            //array('special_price', 'special_from_date', 'special_to_date'), // force update of these attributes
            //$customAttributes,
            $attributes
        ));

        foreach ($originalData as $attributeCode=>$attributeValue) {
            if (!in_array($attributeCode, $attributeCodes)) {
                unset($originalData[$attributeCode]);
            }
        }

        $data = array();
        if (count($originalData) == 0) {
            $this->getServiceLocator()->get('logService')
                ->log(LogService::LEVEL_INFO,
                    $this->getLogCode().'_wrupd_non',
                    'No update required for '.$sku.' but requested was '.implode(', ', $attributes),
                    array('attributes'=>$attributes),
                    array('entity'=>$entity)
                );
        }else{
            /** @var Magento2Service $magento2Service */
            $magento2Service = $this->getServiceLocator()->get('magento2Service');

            foreach ($originalData as $code=>$value) {
                $mappedCode = $magento2Service->getMappedCode('product', $code);
                switch ($mappedCode) {
                    case 'price':
                    case 'special_price':
                    case 'special_from_date':
                    case 'special_to_date':
                        $value = ($value ? $value : NULL);
                    case 'name':
                    case 'description':
                    case 'short_description':
                    case 'weight':
                    case 'barcode':
                    case 'bin_location':
                    case 'msrp':
                    case 'cost':
                        // Same name in both systems
                        $data[$code] = $value;
                        break;
                    case 'enabled':
                        $data['status'] = ($value == 1 ? 1 : 2);
                        break;
                    case 'taxable':
                        $data['tax_class_id'] = ($value == 1 ? 2 : 1);
                        break;
                    case 'visible':
                        $data['visibility'] = ($value == 1 ? 4 : 1);
                        break;
                    // @todo (maybe) : Add logic for this custom attributes
                    case 'brand':
                    case 'size':
                        // Ignore attributes
                        break;
                    case 'product_class':
                    case 'type':
                        if ($type != Update::TYPE_CREATE) {
                            // @todo: Log error(but no exception)
                        }else{
                            // Ignore attributes
                        }
                        break;
                    default:
                        $this->getServiceLocator()->get('logService')
                            ->log(LogService::LEVEL_WARN,
                                $this->getLogCode().'_wr_invdata',
                                'Unsupported attribute for update of '.$sku.': '.$attributeCode,
                               array('attribute'=>$attributeCode),
                               array('entity'=>$entity)
                            );
                        // Warn unsupported attribute
                }
            }

            $localId = $this->_entityService->getLocalId($this->_node->getNodeId(), $entity);

            $storeDataByStoreId = $this->_node->getStoreViews();
            if (count($storeDataByStoreId) > 0 && $type != Update::TYPE_DELETE) {
                $dataPerStore[0] = $data;
                foreach (array('price', 'special_price', 'msrp', 'cost') as $code) {
                    if (array_key_exists($code, $data)) {
                        unset($data[$code]);
                    }
                }

                $websiteIds = array();
                foreach ($storeDataByStoreId as $storeId=>$storeData) {
                    $dataToMap = $magento2Service->mapProductData($data, $storeId, FALSE, TRUE);
                    if ($magento2Service->isStoreUsingDefaults($storeId)) {
                        $dataToCheck = $dataPerStore[0];
                    }else{
                        $dataToCheck = $dataToMap;
                    }

                    $isEnabled = isset($dataToCheck['price']);
                    if ($isEnabled) {
                        $websiteIds[] = $storeData['website_id'];
                        $logCode = $this->getLogCode().'_wrupd_wen';
                        $logMessage = 'enabled';
                    }else{
                        $logCode = $this->getLogCode().'_wrupd_wdis';
                        $logMessage = 'disabled';
                    }

                    $logMessage = 'Product '.$sku.' will be '.$logMessage.' on website '.$storeData['website_id'].'.';
                    $logData = array('store id'=>$storeId, 'dataToMap'=>$dataToMap, 'dataToCheck'=>$dataToCheck);

                    $this->getServiceLocator()->get('logService')
                        ->log(LogService::LEVEL_DEBUGINTERNAL, $logCode, $logMessage, $logData);

                    $dataPerStore[$storeId] = $dataToMap;
                }
                unset($data, $dataToMap, $dataToCheck);

                $storeIds = array_merge(array(0), array_keys($storeDataByStoreId));
                $this->getServiceLocator()->get('logService')->log(LogService::LEVEL_DEBUGINTERNAL,
                    $this->getLogCode().'_wrupd_stor',
                    'StoreIds '.json_encode($storeIds).' (type: '.$type.'), websiteIds '.json_encode($websiteIds).'.',
                    array('store data'=>$storeDataByStoreId)
                );

                foreach ($storeIds as $storeId) {
                    $productData = $dataPerStore[$storeId];
                    $productData['website_ids'] = $websiteIds;

                    if ($magento2Service->isStoreUsingDefaults($storeId)) {
                        $setSpecialPrice = FALSE;
                        unset($productData['special_price']);
                        unset($productData['special_from_date']);
                        unset($productData['special_to_date']);
                    }elseif (isset($productData['special_price'])) {
                        $setSpecialPrice = FALSE;
                    }elseif ($storeId === 0) {
                        $setSpecialPrice = FALSE;
                        $productData['special_price'] = NULL;
                        $productData['special_from_date'] = NULL;
                        $productData['special_to_date'] = NULL;
                    }else{
                        $setSpecialPrice = FALSE;
                        $productData['special_price'] = '';
                        $productData['special_from_date'] = '';
                        $productData['special_to_date'] = '';
                    }

                    $restData = $this->getUpdateDataForSoapCall($productData, $customAttributes);
                    $logData = array(
                        'type'=>$entity->getData('type'),
                        'store id'=>$storeId,
                        'product data'=>$productData,
                    );
                    $restResult = NULL;

                    $updateViaDbApi = ($this->restV1 && $localId && $storeId == 0);
                    if ($updateViaDbApi) {
                        $api = 'DB';
                    }else{
                        $api = 'SOAP';
                    }

                    if ($type == Update::TYPE_UPDATE || $localId) {
                        if ($updateViaDbApi) {
                            try{
                                $tablePrefix = 'catalog_product';
                                $rowsAffected = $this->restV1->updateEntityEav(
                                    $tablePrefix,
                                    $localId,
                                    $entity->getStoreId(),
                                    $productData
                                );

                                if ($rowsAffected != 1) {
                                    throw new MagelinkException($rowsAffected.' rows affected.');
                                }
                            }catch(\Exception $exception) {
                                $this->_entityService->unlinkEntity($nodeId, $entity);
                                $localId = NULL;
                                $updateViaDbApi = FALSE;
                            }
                        }

                        $logMessage = 'Updated product '.$sku.' on store '.$storeId.' ';
                        if ($updateViaDbApi) {
                            $logLevel = LogService::LEVEL_INFO;
                            $logCode = $this->getLogCode().'_wrupddb';
                            $logMessage .= 'successfully via DB api with '.implode(', ', array_keys($productData));
                        }else{
                            try{
                                $putData = $restData; // $sku, $storeId
                                if ($setSpecialPrice) {
                                    $putData['custom_attributes'][] = array(
                                        'attribute_code'=>'special_price',
                                        'value'=>$productData['special_price']
                                    );
                                    $putData['custom_attributes'][] = array(
                                        'attribute_code'=>'special_price_from_date',
                                        'value'=>$productData['special_from_date']
                                    );
                                    $putData['custom_attributes'][] = array(
                                        'attribute_code'=>'special_price',
                                        'value'=>$productData['special_to_date']
                                    );
                                }

                                $restResult = array('update'=>
                                    $this->restV1->put('products/'.$sku, $putData));
                            }catch(\Exception $exception) {
                                $restResult = FALSE;
                                $restFaultMessage = $exception->getPrevious()->getMessage();
                                if (strpos($restFaultMessage, 'Product not exists') !== FALSE) {
                                    $type = Update::TYPE_CREATE;
                                }
                            }

                            $logLevel = ($restResult ? LogService::LEVEL_INFO : LogService::LEVEL_ERROR);
                            $logCode = $this->getLogCode().'_wrupdrest';
                            if ($api != 'SOAP') {
                                $logMessage = $api.' update failed. Removed local id '.$localId
                                    .' from node '.$nodeId.'. '.$logMessage;
                                if (isset($exception)) {
                                    $logData[strtolower($api.' error')] = $exception->getMessage();
                                }
                            }

                            $logMessage .= ($restResult ? 'successfully' : 'without success').' via SOAP api.'
                                .($type == Update::TYPE_CREATE ? ' Try to create now.' : '');
                            $logData['rest data'] = $restData;
                            $logData['rest result'] = $restResult;
                        }
                        $this->getServiceLocator()->get('logService')->log($logLevel, $logCode, $logMessage, $logData);
                    }

                    if ($type == Update::TYPE_CREATE) {

                        $attributeSet = NULL;
                        foreach ($this->_attributeSets as $setId=>$set) {
                            if ($set['name'] == $entity->getData('product_class', 'default')) {
                                $attributeSet = $setId;
                                break;
                            }
                        }
                        if ($attributeSet === NULL) {
                            $message = 'Invalid product class '.$entity->getData('product_class', 'default');
                            throw new \Magelink\Exception\SyncException($message);
                        }

                        $message = 'Creating product(SOAP) : '.$sku.' with '.implode(', ', array_keys($productData));
                        $logData['set'] = $attributeSet;
                        $this->getServiceLocator()->get('logService')
                            ->log(LogService::LEVEL_INFO, $this->getLogCode().'_wr_cr', $message, $logData);

                        $request = array(
                            $entity->getData('type'),
                            $attributeSet,
                            $sku,
                            $restData,
                            $entity->getStoreId()
                        );

                        try{
                            $restResult = $this->restV1->post('products', $parameters);
                            $restFault = NULL;
                        }catch(\Exception $exception) {
                            $restResult = FALSE;
                            $restFault = $exception->getPrevious();
                            $restFaultMessage = $restFault->getMessage();
                            if ($restFaultMessage == 'The value of attribute "SKU" must be unique') {
                                $this->getServiceLocator()->get('logService')
                                    ->log(LogService::LEVEL_WARN,
                                        $this->getLogCode().'_wr_duperr',
                                        'Creating product '.$sku.' hit SKU duplicate fault',
                                        array(),
                                        array('entity'=>$entity, 'rest fault'=>$restFault)
                                    );

                                $check = $this->restV1->get('products/'.$sku, array());
                                if (!$check || !count($check)) {
                                    throw new MagelinkException(
                                        'Magento2 complained duplicate SKU but we cannot find a duplicate!'
                                    );

                                }else{
                                    $found = FALSE;
                                    foreach ($check as $row) {
                                        if ($row['sku'] == $sku) {
                                            $found = TRUE;

                                            $this->_entityService->linkEntity($nodeId, $entity, $row['product_id']);
                                            $this->getServiceLocator()->get('logService')
                                                ->log(LogService::LEVEL_INFO,
                                                    $this->getLogCode().'_wr_dupres',
                                                    'Creating product '.$sku.' resolved SKU duplicate fault',
                                                    array('local_id'=>$row['product_id']),
                                                    array('entity'=>$entity)
                                                );
                                        }
                                    }

                                    if (!$found) {
                                        $message = 'Magento2 found duplicate SKU '.$sku
                                            .' but we could not replicate. Database fault?';
                                        throw new MagelinkException($message);
                                    }
                                }
                            }
                        }

                        if ($restResult) {
                            $this->_entityService->linkEntity($nodeId, $entity, $restResult);
                            $type = Update::TYPE_UPDATE;

                            $logData['rest data'] = $restData;
                            $this->getServiceLocator()->get('logService')->log(LogService::LEVEL_INFO,
                                $this->getLogCode().'_wr_loc_id',
                                'Added product local id '.$restResult.' for '.$sku.' ('.$nodeId.')',
                                $logData
                            );
                        }else{
                            $message = 'Error creating product '.$sku.' in Magento2!';
                            throw new MagelinkException($message, 0, $restFault);
                        }
                    }
                }
                unset($dataPerStore);
            }
        }
    }

    /**
     * Write out the given action.
     * @param Action $action
     * @throws MagelinkException
     */
    public function writeAction(Action $action)
    {
        $entity = $action->getEntity();
        switch($action->getType()) {
            case 'delete':
                $this->restV1->delete('products/'.$entity->getUniqueId());
                $success = TRUE;
                break;
            default:
                throw new MagelinkException('Unsupported action type '.$action->getType().' for Magento2 Orders.');
                $success = FALSE;
        }

        return $success;
    }

}
