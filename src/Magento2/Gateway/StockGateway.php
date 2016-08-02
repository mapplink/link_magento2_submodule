<?php
/**
 * @category Magento2
 * @package Magento2\Gateway
 * @author Andreas Gerhards <andreas@lero9.co.nz>
 * @copyright Copyright (c) 2014 LERO9 Ltd.
 * @license Commercial - All Rights Reserved
 */

namespace Magento2\Gateway;

use Entity\Service\EntityService;
use Entity\Wrapper\Product;
use Log\Service\LogService;
use Magelink\Exception\MagelinkException;
use Node\AbstractNode;
use Node\Entity;


class StockGateway extends AbstractGateway
{
    const GATEWAY_ENTITY = 'stockitem';
    const GATEWAY_ENTITY_CODE = 'si';

    /**
     * Initialize the gateway and perform any setup actions required.
     * @param string $entityType
     * @return bool $success
     * @throws MagelinkException
     */
    protected function _init($entityType)
    {
        $success = parent::_init($entityType);

        if ($entityType != 'stockitem') {
            throw new \Magelink\Exception\MagelinkException('Invalid entity type for this gateway');
            $success = FALSE;
        }

        return $success;
    }

    /**
     * Retrieve and action all updated records (either from polling, pushed data, or other sources).
     */
    public function retrieveEntities()
    {
        if (!$this->_node->getConfig('load_stock')) {
            // No need to retrieve Stock from magento2
            return;
        }

        $this->getNewRetrieveTimestamp();
        $lastRetrieve = $this->getLastRetrieveDate();
        $nodeId = $this->_node->getNodeId();

        $this->getServiceLocator()->get('logService')
            ->log(LogService::LEVEL_INFO,
                $this->getLogCode().'_re_time',
                'Retrieving stockitems updated since '.$lastRetrieve,
                array('type'=>'product', 'timestamp'=>$lastRetrieve)
            );

        /** @var Product[] $products */
        $products = $this->_entityService->locateEntity(
            $nodeId,
            'product',
            0,
            array(),
            array(),
            array('static_field'=>'unique_id')
        );
        $products = array_unique($products);

        $failed = 0;
        foreach ($products as $product) {
            $sku = $product->getUniqueId();

            if (FALSE && $this->db) {
                // TECHNICAL DEBT // ToDo: Implement

            }elseif ($this->restV1) {
                $stockitem = $this->restV1->get('stockItems/'.$sku, array());

                if (is_null($stockitem)) {
                    ++$failed;
                    $this->getServiceLocator()->get('logService')
                        ->log(LogService::LEVEL_WARN,
                            $this->getLogCode().'_re_fail', 'Rest call failed for stockitem '.$sku,
                            array('sku'=>$sku), array('node'=>$this->_node, 'product'=>$product));
                }else{
                    $data = array();
                    $localId = $stockitem->product_id;

                    $data = array('available'=>$stockitem->qty);

                    foreach ($this->_node->getStoreViews() as $storeId=>$storeView) {
                        /** @var bool $needsUpdate */
                        $needsUpdate = TRUE;

                        $parentId = $product->getId();
                        $existingEntity = $this->_entityService
                            ->loadEntityLocal($nodeId, 'stockitem', 0, $localId);

                        if (!$existingEntity) {
                            $existingEntity = $this->_entityService
                                ->loadEntity($nodeId, 'stockitem', $storeId, $sku);

                            if (!$existingEntity) {
                                $existingEntity = $this->_entityService
                                    ->createEntity($nodeId, 'stockitem', $storeId, $sku, $data, $parentId);
                                $this->_entityService->linkEntity($nodeId, $existingEntity, $localId);

                                $this->getServiceLocator()->get('logService')
                                    ->log(LogService::LEVEL_INFO,
                                        $this->getLogCode().'_re_new', 'New stockitem '.$sku,
                                        array('sku'=>$sku), array('node'=>$this->_node, 'stockitem'=>$existingEntity));
                                $needsUpdate = FALSE;
                            }else {
                                $this->getServiceLocator()->get('logService')
                                    ->log(LogService::LEVEL_INFO,
                                        $this->getLogCode().'_re_link', 'Unlinked stockitem '.$sku,
                                        array('sku'=>$sku), array('node'=>$this->_node, 'stockitem'=>$existingEntity)
                                    );
                                $this->_entityService->linkEntity($nodeId, $existingEntity, $localId);
                            }
                        }else {
                            $this->getServiceLocator()->get('logService')
                                ->log(LogService::LEVEL_INFO,
                                    $this->getLogCode().'_re_upd', 'Updated stockitem '.$sku,
                                    array('sku'=>$sku), array('node'=>$this->_node, 'stockitem'=>$existingEntity)
                                );
                        }
                        if ($needsUpdate) {
                            $this->_entityService->updateEntity($nodeId, $existingEntity, $data, FALSE);
                        }
                    }
                }
            }else{
                throw new \Magelink\Exception\NodeException('No valid API available for sync');
            }
        }

        $this->_nodeService
            ->setTimestamp($this->_nodeEntity->getNodeId(), 'stockitem', 'retrieve', $this->getNewRetrieveTimestamp());

        $seconds = ceil($this->getAdjustedTimestamp() - $this->getNewRetrieveTimestamp());
        $message = 'Retrieved '.count($products).' stockitems in '.$seconds.'s up to '
            .strftime('%H:%M:%S, %d/%m', $this->retrieveTimestamp).'.';
        $logData = array('type'=>'stockitem', 'total no'=>count($products), 'failed'=>$failed, 'period [s]'=>$seconds);
        if (count($products) > 0) {
            $logData['per entity [s]'] = round($seconds / count($products), 3);
        }
        $this->getServiceLocator()->get('logService')->log(LogService::LEVEL_INFO, $this->getLogCode().'_re_no', $message, $logData);
    }

    /**
     * @param \Entity\Entity $entity
     * @param bool $log
     * @param bool $error
     * @return int|NULL
     */
    protected function getParentLocal(\Entity\Entity $entity, $log = FALSE, $error = FALSE)
    {
        $nodeId = $this->_node->getNodeId();
        $localId = $this->_entityService->getLocalId($this->_node->getNodeId(), $entity->getParentId());

        $logCode = $this->getLogCode().'_plo';
        $logLevel = ($error ? LogService::LEVEL_ERROR : LogService::LEVEL_WARN);
        if ($log || $error) {
            $logMessage = 'Stock update for '.$entity->getUniqueId().' ('.$nodeId.') had to use parent local!';
            $this->getServiceLocator()->get('logService')
                ->log($logLevel, $logCode, $logMessage,
                    array('parent'=>$entity->getParentId(), 'local id'=>$localId),
                    array('node'=>$this->_node, 'entity'=>$entity)
                );
        }

        if (!$localId) {
            $parentEntity = $entity->getParent();
            if ($this->restV1) {
                $localId = $this->restV1->getLocalId('product', $parentEntity->getUniqueId());
            }

            if (!$localId && $this->restV1) {
                $productInfo = $this->restV1->get('products/'.$parentEntity->getUniqueId(), array());
                $localId = $productInfo['id'];
            }

            if ($localId) {
                $this->_entityService->linkEntity($this->_node->getNodeId(), $parentEntity, $localId);
                if ($log) {
                    $this->getServiceLocator()->get('logService')->log(LogService::LEVEL_INFO, $logCode.'_relnk',
                        'Stock parent product '.$entity->getUniqueId().' re-linked on '.$nodeId.'!', array());
                }
            }elseif ($log || $error) {
                $this->getServiceLocator()->get('logService')->log($logLevel, $logCode.'_nolnk',
                    'Stock update for '.$entity->getUniqueId().' on node '.$nodeId.': Parent had no local id!',
                    array('data'=>$entity->getFullArrayCopy()), array('node'=>$this->_node));
            }
        }

        return $localId;
    }

    /**
     * Write out all the updates to the given entity.
     * @param \Entity\Entity $entity
     * @param string[] $attributes
     * @param int $type
     * @throws MagelinkException
     */
    public function writeUpdates(\Entity\Entity $entity, $attributes, $type=\Entity\Update::TYPE_UPDATE)
    {
        $logCode = $this->getLogCode();
        $logData = array('data'=>$entity->getAllSetData());
        $logEntities = array('node'=>$this->_node, 'entity'=>$entity);

        if (in_array('available', $attributes)) {
            $isUnlinked = FALSE;
            $nodeId = $this->_node->getNodeId();
            $logEntities = array('node'=>$this->_node, 'stockitem'=>$entity);

            $localId = $this->_entityService->getLocalId($nodeId, $entity);
            $qty = $entity->getData('available');
            $isInStock = (int) ($qty > 0);

            do {
                $success = FALSE;
                if ($localId) {
                    if ($this->restV1) {
                        $success = $this->restV1->put(
                            'products/'.$entity->getUniqueId().'/stockItems/'.$localId,
                            array('qty'=>$qty, 'is_in_stock'=>($isInStock))
                        );
                    }
                }

                $quit = $success || $isUnlinked;
                $logData = array('node id'=>$nodeId, 'local id'=>$localId, 'data'=>$entity->getFullArrayCopy());

                if (!$success) {
                    if (!$isUnlinked) {
                        if ($localId) {
                            $this->_entityService->unlinkEntity($this->_node->getNodeId(), $entity);
                        }
                        $isUnlinked = TRUE;
                        $localId = $this->getParentLocal($entity, TRUE);

                        $this->getServiceLocator()->get('logService')
                            ->log(LogService::LEVEL_WARN,
                                $logCode.'_unlink',
                                'Removed stockitem local id from '.$entity->getUniqueId().' ('.$nodeId.')',
                                $logData, $logEntities
                            );
                    }else{
                        $product = $this->_entityService
                            ->loadEntityId($this->_node->getNodeId(), $entity->getParentId());

                        if ($localId) {
                            $localId = NULL;
                            $this->_entityService->unlinkEntity($this->_node->getNodeId(), $product);

                            $logMessage = 'Stock update for '.$entity->getUniqueId().' failed!'
                                .' Product '.$product->getUniqueId().' had wrong local id '.$localId.' ('.$nodeId.').'
                                .' Both local ids (on stockitem and product) are now removed.';
                            $this->getServiceLocator()->get('logService')->log(LogService::LEVEL_ERROR,
                                $this->getLogCode().'_par_unlink', $logMessage, $logData, $logEntities);
                        }
                    }
                }
            }while (!$quit);

            if ($isUnlinked) {
                if ($success) {
                    $this->_entityService->linkEntity($this->_node->getNodeId(), $entity, $localId);
                    $logLevel = LogService::LEVEL_INFO;
                    $logCode .= '_link';
                    $logMessage = 'Linked stockitem '.$entity->getUniqueId().' on node '.$nodeId;
                }else{
                    $logLevel = LogService::LEVEL_WARN;
                    $logCode .= '_link_fail';
                    $logMessage = 'Stockitem '.$entity->getUniqueId().' could not be linked on node '.$nodeId;
                }
                $this->getServiceLocator()->get('logService')
                    ->log($logLevel, $logCode, $logMessage, $logData, $logEntities);
            }
        }else{
            // We don't care about any other attributes
            $success = TRUE;
        }

        return $success;
    }

    /**
     * Write out the given action.
     * @param \Entity\Action $action
     * @throws MagelinkException
     */
    public function writeAction(\Entity\Action $action)
    {
        throw new MagelinkException('Unsupported action type ' . $action->getType() . ' for Magento2 Stock Items.');
    }

}
