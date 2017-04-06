<?php
/**
 * Magento2\Gateway\CustomerGateway
 *
 * @category Magento2
 * @package Magento2\Gateway
 * @author Andreas Gerhards <andreas@lero9.co.nz>
 * @copyright Copyright (c) 2014 LERO9 Ltd.
 * @license http://opensource.org/licenses/BSD-3-Clause BSD-3-Clause - Please view LICENSE.md for more information
 */

namespace Magento2\Gateway;

use Entity\Service\EntityService;
use Log\Service\LogService;
use Magelink\Exception\MagelinkException;
use Magelink\Exception\NodeException;
use Magelink\Exception\GatewayException;


class CustomerGateway extends AbstractGateway
{
    const GATEWAY_ENTITY = 'customer';
    const GATEWAY_ENTITY_CODE = 'cu';

    /** @var  array $this->customerGroups */
    protected $customerGroups;


    /**
     * Initialize the gateway and perform any setup actions required.
     * @param string $entityType
     * @return bool $success
     * @throws GatewayException
     */
    protected function _init($entityType)
    {
        $success = parent::_init($entityType);

        if ($entityType != 'customer') {
            throw new GatewayException('Invalid entity type for this gateway');
            $success = FALSE;
        }else{
            if ($this->_node->getConfig('customer_attributes')
                && strlen($this->_node->getConfig('customer_attributes'))) {

                $this->restV1 = $this->_node->getApi('restV1');
                if (!$this->restV1) {
                    throw new GatewayException('SOAP v1 is required for extended customer attributes');
                }
            }

            try {
                $groups = $this->restV1->get('customerGroups/search', array(
                    'filter'=>array(array(
                        'field'=>'id',
                        'value'=>0,
                        'condition_type'=>'gt'
                    ))
                ));
            }catch (\Exception $exception) {
                throw new GatewayException($exception->getMessage(), $exception->getCode(), $exception);
                $succes = FALSE;
            }

            $this->customerGroups = array();
            foreach ($groups as $group) {
                $this->customerGroups[$group['id']] = $group;
            }
        }

        return $success;
    }

    /**
     * Retrieve and action all updated records(either from polling, pushed data, or other sources).
     * @return int $numberOfRetrievedEntities
     * @throws GatewayException
     * @throws NodeException
     */
    public function retrieveEntities()
    {
        $this->getServiceLocator()->get('logService')
            ->log(LogService::LEVEL_INFO,
                $this->getLogCode().'_re_time',
                'Retrieving customers updated since '.$this->getLastRetrieveDate(),
                array('type'=>'customer', 'timestamp'=>$this->getLastRetrieveDate())
            );

        if ($this->restV1) {
            try {
                $results = $this->restV1->get('customers/search', array(
                    'filter'=>array(array(
                        'field'=>'updated_at',
                        'value'=>$this->getLastRetrieveDate(),
                        'condition_type'=>'gt'
                    ))
                ));
            }catch (\Exception $exception) {
                throw new GatewayException($exception->getMessage(), $exception->getCode(), $exception);
            }

            if (!is_array($results)) {
                $this->getServiceLocator()->get('logService')->log(LogService::LEVEL_ERROR,
                    $this->getLogCode().'_re_rest',
                    'RESTv1 (customers/search) did not return an array but '.gettype($results).' instead.',
                    array('type'=>gettype($results), 'class'=>(is_object($results) ? get_class($results) : 'no object')),
                    array('restV1 result'=>$results)
                );
            }
/*
            $specialAtt = $this->_node->getConfig('customer_special_attributes');
            if (!strlen(trim($specialAtt))) {
                $specialAtt = false;
            }else{
                $specialAtt = trim(strtolower($specialAtt));
                if (!$this->entityConfigService->checkAttribute('customer', $specialAtt)) {
                    $this->entityConfigService->createAttribute(
                        $specialAtt, $specialAtt, 0, 'varchar', 'customer', 'Custom Magento2 attribute (special - taxvat)');
                    $this->getServiceLocator()->get('nodeService')
                        ->subscribeAttribute($this->_node->getNodeId(), $specialAtt, 'customer');
                }
            }
*/
            $additionalAttributes = $this->_node->getConfig('customer_attributes');
            if (is_string($additionalAttributes)) {
                $additionalAttributes = explode(',', $additionalAttributes);
            }
            if (!$additionalAttributes || !is_array($additionalAttributes)) {
                $additionalAttributes = array();
            }

            foreach ($additionalAttributes as $key=>&$attributeCode) {
                $attributeCode = trim(strtolower($attributeCode));
                if (!strlen($attributeCode)) {
                    unset($additionalAttributes[$key]);
                }else{
                    if (!$this->entityConfigService->checkAttribute('customer', $attributeCode)) {
                        $this->entityConfigService->createAttribute(
                            $attributeCode, $attributeCode, 0, 'varchar', 'customer', 'Custom Magento2 attribute');
                        $this->getServiceLocator()->get('nodeService')
                            ->subscribeAttribute($this->_node->getNodeId(), $attributeCode, 'customer');
                    }
                }
            }

            foreach ($results as $customerData) {
                $data = array();

                $uniqueId = $customerData['email'];
                $localId = $customerData['id'];
                $storeId = ($this->_node->isMultiStore() ? $customerData['store_id'] : 0);
                $parentId = NULL;

                $data['first_name'] = (isset($customerData['firstname']) ? $customerData['firstname'] : NULL);
                $data['middle_name'] = (isset($customerData['middlename']) ? $customerData['middlename'] : NULL);
                $data['last_name'] = (isset($customerData['lastname']) ? $customerData['lastname'] : NULL);
                $data['date_of_birth'] = (isset($customerData['dob']) ? $customerData['dob'] : NULL);

/*                if ($specialAtt) {
                    $data[$specialAtt] = (isset($customerData['taxvat']) ? $customerData['taxvat'] : NULL);
                }
                if (count($additionalAttributes) && $this->restV1) {
                    // TECHNICAL DEBT // ToDo: Extract extra information from the first REST call ($results)
                    foreach ($additionalAttributes as $attributeCode) {
                        if (array_key_exists($attributeCode, $extra)) {
                            $data[$attributeCode] = $extra[$attributeCode];
                        }else{
                            $data[$attributeCode] = NULL;
                        }
                    }
                }
*/
                if (isset($this->customerGroups[intval($customerData['group_id'])])) {
                    $data['customer_type'] = $this->customerGroups[intval($customerData['group_id'])]['code'];
                }else{
                    $this->getServiceLocator()->get('logService')
                        ->log(LogService::LEVEL_WARN,
                            $this->getLogCode().'_ukwn_grp',
                            'Unknown customer group ID '.$customerData['group_id'],
                            array('group'=>$customerData['group_id'], 'unique'=>$customerData['email'])
                        );
                }

                $existingEntity = $this->_entityService
                    ->loadEntityLocal($this->_node->getNodeId(), 'customer', 0, $localId);
                if (!$existingEntity) {
                    $existingEntity = $this->_entityService
                        ->loadEntity($this->_node->getNodeId(), 'customer', $storeId, $uniqueId);
                    if (!$existingEntity) {
                        $existingEntity = $this->_entityService->createEntity(
                            $this->_node->getNodeId(),
                            'customer',
                            $storeId,
                            $uniqueId,
                            $data,
                            $parentId
                        );
                        $this->_entityService->linkEntity($this->_node->getNodeId(), $existingEntity, $localId);
                        $this->getServiceLocator()->get('logService')
                            ->log(LogService::LEVEL_INFO,
                                $this->getLogCode().'_new',
                                'New customer '.$uniqueId,
                                array('code'=>$uniqueId),
                                array('node'=>$this->_node, 'entity'=>$existingEntity)
                            );
                    }elseif ($this->_entityService->getLocalId($this->_node->getNodeId(), $existingEntity) != NULL) {
                        $this->getServiceLocator()->get('logService')
                            ->log(LogService::LEVEL_INFO,
                                $this->getLogCode().'_relink',
                                'Incorrectly linked customer '.$uniqueId,
                                array('code'=>$uniqueId),
                                array('node'=>$this->_node, 'entity'=>$existingEntity)
                            );
                        $this->_entityService->unlinkEntity($this->_node->getNodeId(), $existingEntity);
                        $this->_entityService->linkEntity($this->_node->getNodeId(), $existingEntity, $localId);
                    }else{
                        $this->getServiceLocator()->get('logService')
                            ->log(LogService::LEVEL_INFO,
                                $this->getLogCode().'_link',
                                'Unlinked customer '.$uniqueId,
                                array('code'=>$uniqueId),
                                array('node'=>$this->_node, 'entity'=>$existingEntity)
                            );
                        $this->_entityService->linkEntity($this->_node->getNodeId(), $existingEntity, $localId);
                    }
                }else{
                    $this->getServiceLocator()->get('logService')
                        ->log(LogService::LEVEL_INFO,
                            $this->getLogCode().'_upd',
                            'Updated customer '.$uniqueId,
                            array('code'=>$uniqueId),
                            array('node'=>$this->_node, 'entity'=>$existingEntity)
                        );
                }

                if ($this->_node->getConfig('load_full_customer')) {
                    $customerData['magelink_entity_id'] = $existingEntity->getId();
                    $data = array_merge($data, $this->createAddresses($customerData));

                    if ($this->db) {
                        try {
                            $data['enable_newsletter'] = (int) $this->db->getNewsletterStatus($localId);
                        }catch (\Exception $exception) {
                            throw new GatewayException($exception->getMessage(), $exception->getCode(), $exception);
                        }
                    }
                }

                $this->_entityService->updateEntity($this->_node->getNodeId(), $existingEntity, $data, FALSE);
            }
        }else{
            // Nothing worked
            throw new NodeException('No valid API available for sync');
        }

        return count($results);
    }

    /**
     * Create the Address entities for a given customer and pass them back as the appropriate attributes
     * @param array $customerData
     * @return array $data
     * @throws GatewayException
     */
    protected function createAddresses($customer)
    {
        $data = array();

        foreach ($customer['addresses']  as $address) {
            if (isset($address['default_billing']) && $address['default_billing']) {
                $data['billing_address'] = $this->createAddressEntity($address, $customer, 'billing');
            }
            if (isset($address['default_shipping']) && $address['default_shipping']) {
                $data['shipping_address'] = $this->createAddressEntity($address, $customer, 'shipping');
            }
            if (!isset($data['billing_address']) && !isset($data['shipping_address'])) {
                // TECHNICAL DEBT // ToDo: Store this maybe? For now ignore
            }
        }

        return $data;
    }

    /**
     * Create an individual Address entity for a customer
     *
     * @param array $addressData
     * @param \Entity\Entity $customer
     * @param string $type "billing" or "shipping"
     * @return \Entity\Entity|NULL $addressEntity
     * @throws MagelinkException
     * @throws NodeException
     */
    protected function createAddressEntity(array $addressData, $customer, $type)
    {
        $uniqueId = 'cust-'.$customer['id'].'-'.$type;

        $addressEntity = $this->_entityService->loadEntity(
            $this->_node->getNodeId(),
            'address',
            ($this->_node->isMultiStore() ? $customer->store_id : 0),
            $uniqueId
        );

        if (isset($addressData['street']) && is_string($addressData['street'])) {
            $street = $addressData['street'];
        }elseif (isset($addressData['street']) && is_array($addressData['street'])) {
            $street = implode(chr(10), $addressData['street']);
        }else{
            $street = NULL;
        }

        $data = array(
            'first_name'=>(isset($addressData['firstname']) ? $addressData['firstname'] : NULL),
            'middle_name'=>(isset($addressData['middlename']) ? $addressData['middlename'] : NULL),
            'last_name'=>(isset($addressData['lastname']) ? $addressData['lastname'] : NULL),
            'prefix'=>(isset($addressData['prefix']) ? $addressData['prefix'] : NULL),
            'suffix'=>(isset($addressData['suffix']) ? $addressData['suffix'] : NULL),
            'street'=>$street,
            'city'=>(isset($addressData['city']) ? $addressData['city'] : NULL),
            'region'=>(isset($addressData['region']->region) ? $addressData['region']->region : NULL),
            'postcode'=>(isset($addressData['postcode']) ? $addressData['postcode'] : NULL),
            'country_code'=>(isset($addressData['country_id']) ? $addressData['country_id'] : NULL),
            'telephone'=>(isset($addressData['telephone']) ? $addressData['telephone'] : NULL),
            'company'=>(isset($addressData['company']) ? $addressData['company'] : NULL)
        );

        if (!$addressEntity) {
            $addressEntity = $this->_entityService->createEntity(
                $this->_node->getNodeId(),
                'address',
                ($this->_node->isMultiStore() ? $customer->store_id : 0),
                $uniqueId,
                $data,
                $customer['magelink_entity_id']
            );
            $this->_entityService->linkEntity($this->_node->getNodeId(), $addressEntity, $addressData['id']);
        }else{
            $this->_entityService->updateEntity($this->_node->getNodeId(), $addressEntity, $data, FALSE);
        }

        return $addressEntity;
    }

    /**
     * Write out all the updates to the given entity.
     * @param \Entity\Entity $entity
     * @param \Entity\Attribute[] $attributes
     * @param int $type
     * @return bool
     */
    public function writeUpdates(\Entity\Entity $entity, $attributes, $type = \Entity\Update::TYPE_UPDATE)
    {
        return NULL;

        // TECHNICAL DEBT // ToDo: Implement writeUpdates() method.

/*
        $additionalAttributes = $this->_node->getConfig('customer_attributes');
        if (is_string($additionalAttributes)) {
            $additionalAttributes = explode(',', $additionalAttributes);
        }
        if (!$additionalAttributes || !is_array($additionalAttributes)) {
            $additionalAttributes = array();
        }

        $data = array(
            'additional_attributes'=>array(
                'single_data'=>array(),
                'multi_data'=>array(),
            ),
        );

        foreach($attributes as $att) {
            $v = $entity->getData($att);
            if (in_array($att, $additionalAttributes)) {
                // Custom attribute
                if (is_array($v)) {
                    // TECHNICAL DEBT // ToDo implement
                    throw new MagelinkException('This gateway does not yet support multi_data additional attributes');
                }else{
                    $data['additional_attributes']['single_data'][] = array(
                        'key'=>$att,
                        'value'=>$v,
                    );
                }
                continue;
            }
            // Normal attribute
            switch($att) {
                case 'name':
                case 'description':
                case 'short_description':
                case 'price':
                case 'special_price':
                    // Same name in both systems
                    $data[$att] = $v;
                    break;
                case 'special_from':
                    $data['special_from_date'] = $v;
                    break;
                case 'special_to':
                    $data['special_to_date'] = $v;
                    break;
                case 'customer_class':
                    if ($type != \Entity\Update::TYPE_CREATE) {
                        // TECHNICAL DEBT // ToDo log error (but no exception)
                    }
                    break;
                case 'type':
                    if ($type != \Entity\Update::TYPE_CREATE) {
                        // TECHNICAL DEBT // ToDo log error (but no exception)
                    }
                    break;
                case 'enabled':
                    $data['status'] = ($v == 1 ? 2 : 1);
                    break;
                case 'visible':
                    $data['status'] = ($v == 1 ? 4 : 1);
                    break;
                case 'taxable':
                    $data['status'] = ($v == 1 ? 2 : 1);
                    break;
                default:
                    // Warn unsupported attribute
                    break;
            }
        }

        if ($type == \Entity\Update::TYPE_UPDATE) {
            $req = array(
                $entity->getUniqueId(),
                $data,
                $entity->getStoreId(),
                'sku'
            );
            $this->restV1->call('catalogCustomerUpdate', $req);
        }else if ($type == \Entity\Update::TYPE_CREATE) {

            $attSet = NULL;
            foreach($this->_attSets as $setId=>$set) {
                if ($set['name'] == $entity->getData('customer_class', 'default')) {
                    $attSet = $setId;
                    break;
                }
            }
            $req = array(
                $entity->getData('type'),
                $attSet,
                $entity->getUniqueId(),
                $data,
                $entity->getStoreId()
            );
            $res = $this->restV1->call('catalogCustomerCreate', $req);
            if (!$res) {
                throw new MagelinkException('Error creating customer in Magento2 (' . $entity->getUniqueId() . '!');
            }
            $this->_entityService->linkEntity($this->_node->getNodeId(), $entity, $res);
        }
*/
    }

    /**
     * Write out the given action.
     * @param \Entity\Action $action
     * @return bool
     */
    public function writeAction(\Entity\Action $action)
    {
        return NULL;

/*
        $entity = $action->getEntity();

        switch($action->getType()) {
            case 'delete':
                $this->restV1->call('catalogCustomerDelete', array($entity->getUniqueId(), 'sku'));
                break;
            default:
                throw new MagelinkException('Unsupported action type ' . $action->getType() . ' for Magento2 Orders.');
        }
*/
    }

}
