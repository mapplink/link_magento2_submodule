<?php
/**
 * Magento2\Service
 * @category Magento2
 * @package Magento2\Service
 * @author Andreas Gerhards <andreas@lero9.co.nz>
 * @copyright Copyright (c) 2014 LERO9 Ltd.
 * @license http://opensource.org/licenses/BSD-3-Clause BSD-3-Clause - Please view LICENSE.md for more information
 */

namespace Magento2\Service;

use Application\Service\ApplicationConfigService;
use Log\Service\LogService;
use Magelink\Exception\GatewayException;


class Magento2ConfigService extends ApplicationConfigService
{

    /**
     * @return array $defaultStores
     */
    public function getDefaultStores()
    {
        return $this->getConfigData('default_stores');
    }

    /**
     * @return array $storeLimits
     */
    public function getStoreLimits()
    {
        $storeLimits = array();
        foreach ($this->getConfigData('store_limits') as $storeId=>$limits) {
            if (is_int($storeId) && is_array($limits) && count($limits) == 2) {
                $storeLimits[$storeId] = $limits;
            }
        }

        return $storeLimits;
    }

    /**
     * @return array $pendingProcessingLimits
     */
    public function getStoreLimitForPendingProcessing()
    {
        $storeLimits = $this->getConfigData('store_limits');
        if (isset($storeLimits['pending_processing'])
          && is_array($storeLimits['pending_processing']) && count($storeLimits['pending_processing'] == 2)) {
            $pendingProcessingLimits = $storeLimits['pending_processing'];
        }else{
            $pendingProcessingLimits = array();
        }

        return $pendingProcessingLimits;
    }

    /**
     * @return array $storeBaseCurrencies
     */
    protected function getStoreCurrencies()
    {
        return $this->getConfigData('store_currencies');
    }

    /**
     * @param int $storeId
     * @return string $baseCurrencyString
     */
    public function getBaseCurrency($storeId)
    {
        $storeCurrencies = $this->getStoreCurrencies();

        if ($storeId != (int) $storeId) {
            $this->getServiceLocator()->get('logService')->log(LogService::LEVEL_ERROR,
                'mg2_csvc_bc_stid',
                'Invalid call of getBaseCurrency.',
                array('store id'=>$storeId, 'store currencies'=>$storeCurrencies)
            );
        }

        if (is_array($storeCurrencies) && array_key_exists($storeId, $storeCurrencies) && $storeCurrencies[$storeId]) {
            $baseCurrencyString = $storeCurrencies[$storeId];
        }elseif (is_array($storeCurrencies) && array_key_exists(0, $storeCurrencies) && $storeCurrencies[0]) {
            $baseCurrencyString = $storeCurrencies[0];
        }else{
            $baseCurrencyString = NULL;
            new GatewayException('The store currency configuration is not valid. (Called with storeId '.$storeId.'.)');
        }

        return $baseCurrencyString;
    }

    /**
     * @return array $storeMap
     */
    protected function getStoreMap()
    {
        return $this->getConfigData('store_map');
    }

    /**
     * @param string $entityType
     * @param int $storeId
     * @param bool $readFromManento
     * @return array $productMap
     */
    public function getMapByStoreId($entityType, $storeId, $readFromMagento)
    {
        $map = array();
        $storeMap = $this->getStoreMap();

        if (!is_numeric($storeId) && $readFromMagento ) {
            new GatewayException('That is not a valid call for store map with no store id and reading from Magento2.');
        }else{
            foreach ($storeMap as $id=>$mapPreStore) {
                if (!$readFromMagento) {
                    $id = abs($id);
                }
                if ($storeId === FALSE || $storeId == $id && isset($mapPreStore[$entityType])) {
                    $mapPerStoreAndEntityType = $mapPreStore[$entityType];
                    $flippedMap = array_flip($mapPerStoreAndEntityType);

                    if (!is_array($mapPerStoreAndEntityType) || count($mapPerStoreAndEntityType) != count($flippedMap)) {
                        $message = 'There is no valid '.$entityType.' map';
                        if ($storeId !== FALSE) {
                            $message .= ' for store '.$storeId;
                        }
                        new GatewayException($message.'.');
                    }elseif ($readFromMagento) {
                        $map = array_replace_recursive($mapPerStoreAndEntityType, $map);
                    }else{
                        $map = array_replace_recursive($flippedMap, $map);
                    }
                }
            }
        }

        return $map;
    }

    /**
     * @return array $entityVariables
     */
    protected function getEntityVariables()
    {
        return $this->getConfigData('entity_variables');
    }

    /**
     * @param int $attributeSetId
     * @return array $productVariables
     */
    public function getProductAttributeVariables($attributeSetId)
    {
        $productVariablesPerAttributeSetId = $this->getArrayKeyData($this->getEntityVariables(), 'product');
        return $this->getArrayKeyData($productVariablesPerAttributeSetId, $attributeSetId);
    }

}
