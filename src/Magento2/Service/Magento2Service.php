<?php
/**
 * Magento2\Service
 * @category Magento2
 * @package Magento2\Service
 * @author Andreas Gerhards <andreas@lero9.co.nz>
 * @copyright Copyright (c) 2014 LERO9 Ltd.
 * @license Commercial - All Rights Reserved
 */

namespace Magento2\Service;

use Log\Service\LogService;
use Magelink\Exception\GatewayException;
use Magelink\Exception\MagelinkException;
use Magelink\Exception\NodeException;
use Zend\Db\TableGateway\TableGateway;
use Zend\ServiceManager\ServiceLocatorAwareInterface;
use Zend\ServiceManager\ServiceLocatorInterface;


class Magento2Service implements ServiceLocatorAwareInterface
{

    const PRODUCT_TYPE_VIRTUAL = 'virtual';
    const PRODUCT_TYPE_DOWNLOADABLE = 'downloadable';
    const PRODUCT_TYPE_GIFTCARD = 'giftcard';

    /** @var ServiceLocatorInterface */
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
     * Check if product is shippable
     * @param string $productType
     * @return bool
     */
    public function isProductTypeShippable($productType)
    {
        $notShippableTypes = array(
            self::PRODUCT_TYPE_VIRTUAL,
            self::PRODUCT_TYPE_DOWNLOADABLE,
            self::PRODUCT_TYPE_GIFTCARD
        );

        $isShippable = !in_array($productType, $notShippableTypes);
        return $isShippable;
    }

    /**
     * @param int $storeId
     * @return array $cleanData
     */
    public function isStoreUsingDefaults($storeId)
    {
        $defaultStores = $this->getServiceLocator()->get('magento2ConfigService')->getDefaultStores();
        return in_array($storeId, $defaultStores);
    }

    /**
     * @param string $entityType
     * @param string $code
     * @param bool $readFromMagento
     * @return array $mappedCode
     */
    public function getMappedCode($entityType, $code)
    {
        /** @var \Magento2\Service\Magento2ConfigService $configService */
        $configService = $this->getServiceLocator()->get('magento2ConfigService');
        $map = $configService->getMapByStoreId($entityType, FALSE, FALSE);

        if (array_key_exists($code, $map)) {
            $mappedCode = $map[$code];
        }else{
            $mappedCode = $code;
        }

        return $mappedCode;
    }

    /**
     * @param string $entityType
     * @param int $storeId
     * @param bool $readFromMagento
     * @return array $storeMap
     */
    protected function getStoreMapById($entityType, $storeId, $readFromMagento)
    {
        /** @var \Magento2\Service\Magento2ConfigService $configService */
        $configService = $this->getServiceLocator()->get('magento2ConfigService');
        $map = $configService->getMapByStoreId($entityType, $storeId, $readFromMagento);

        return $map;
    }

    /**
     * @param string $entityType
     * @param array $data
     * @return array $cleanData
     * @throws GatewayException
     */
    protected function cleanData($entityType, array $data)
    {
        /** @var \Magento2\Service\Magento2ConfigService $configService */
        $configService = $this->getServiceLocator()->get('magento2ConfigService');

        $originalData = $data;
        $map = $configService->getMapByStoreId($entityType, FALSE, FALSE);

        foreach ($map as $toRemove=>$toKeep) {
            if (array_key_exists($toRemove, $data)) {
                unset($data[$toRemove]);
            }
        }

        $this->getServiceLocator()->get('logService')->log(LogService::LEVEL_DEBUGINTERNAL, 'mag_svc_cleanDat',
            'Cleaned '.json_encode($originalData).' to '.json_encode($data).'.', array('to remove'=>array_keys($map)));

        return $data;
    }

    /**
     * @param string $entityType
     * @param array $data
     * @param int $storeId
     * @param bool $readFromMagento
     * @param bool $override
     * @return array $mappedData
     * @throws GatewayException
     */
    protected function mapData($entityType, array $data, $storeId, $readFromMagento, $override)
    {
        $originalData = $data;
        $map = $this->getStoreMapById($entityType, $storeId, $readFromMagento);

        foreach ($map as $mapFrom=>$mapTo) {
            if (array_key_exists($mapTo, $data) && !$override) {
                $message = 'Re-mapping from '.$mapFrom.' to '.$mapTo.' failed because key is already existing in '
                    .$entityType.' data: '.implode(', ', array_keys($data)).'.';
                throw new GatewayException($message);
            }elseif (array_key_exists($mapFrom, $data)) {
                $data[$mapTo] = $data[$mapFrom];
                unset($data[$mapFrom]);
            }
        }

        if (!$readFromMagento) {
            $data = $this->cleanData($entityType, $data);
        }

        $this->getServiceLocator()->get('logService')->log(LogService::LEVEL_DEBUGINTERNAL, 'mag_svc_mapData',
            'Mapped '.json_encode($originalData).' to '.json_encode($data).'.', array('map from=>to'=>$map));

        return $data;
    }

    /**
     * @param array $productData
     * @param int $storeId
     * @param bool|true $readFromMagento
     * @param bool|false $override
     * @return array $mappedProductData
     */
    public function mapProductData(array $productData, $storeId, $readFromMagento = TRUE, $override = FALSE)
    {
        $mappedProductData = $this->mapData('product', $productData, $storeId, $readFromMagento, $override);
        return $mappedProductData;
    }

    /**
     * @param array $data
     * @param float $baseToCurrencyRate
     * @return array
     */
    protected function addCurrencyFromBase(array $data, $baseToCurrencyRate)
    {
        $newData = array();

        foreach ($data as $key=>$value) {
            if (is_array($value)) {
                $value = $this->addCurrencyFromBase($value, $baseToCurrencyRate);
            }

            $newData[$key] = $value;

            if (strpos($key, 'base_') === 0) {
                $newKey = str_replace('base_', '', $key);
                $newData[$newKey] = $value * $baseToCurrencyRate;
            }
        }

        return $newData;
    }

}
