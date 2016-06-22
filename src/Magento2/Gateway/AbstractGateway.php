<?php
/**
 * Magento2 Abstract Gateway
 * @category Magento2
 * @package Magento2\Api
 * @author Andreas Gerhards <andreas@lero9.co.nz>
 * @copyright Copyright (c) 2014 LERO9 Ltd.
 * @license Commercial - All Rights Reserved
 */

namespace Magento2\Gateway;

use Magelink\Exception\MagelinkException;
use Magelink\Exception\GatewayException;
use Node\AbstractGateway as BaseAbstractGateway;
use Node\AbstractNode;
use Node\Entity;


abstract class AbstractGateway extends BaseAbstractGateway
{
    const GATEWAY_NODE_CODE = 'mg2';
    const GATEWAY_ENTITY_CODE = 'gey';
    const GATEWAY_ENTITY = 'generic';

    /** @var \Entity\Service\EntityConfigService $this->entityConfigService */
    protected $entityConfigService = NULL;

    /** @var \Magento2\Api\Db $this->db */
    protected $db = NULL;
    /** @var \Magento2\Api\RestV1 $this->restV1 */
    protected $restV1 = NULL;


    /**
     * Initialize the gateway and perform any setup actions required.
     * @param string $entityType
     * @throws MagelinkException
     * @return bool $success
     */
    protected function _init($entityType)
    {
        $this->entityConfigService = $this->getServiceLocator()->get('entityConfigService');

        $this->db = $this->_node->getApi('db');
        $this->restV1 = $this->_node->getApi('restV1');

        if (!$this->restV1) {
            throw new GatewayException('RESTv1 is required for Magento2 '.ucfirst($entityType));
            $success = FALSE;
        }else{
            $this->apiOverlappingSeconds += $this->_node->getConfig('api_overlapping_seconds');
            $success = TRUE;
        }

        return $success;
    }

}
