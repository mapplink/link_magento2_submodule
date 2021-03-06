<?php
/**
 * @category Magento2
 * @package Magento2
 * @author Andreas Gerhards <andreas@lero9.co.nz>
 * @copyright Copyright (c) 2014 LERO9 Ltd.
 * @license http://opensource.org/licenses/BSD-3-Clause BSD-3-Clause - Please view LICENSE.md for more information
 */

namespace Magento2;

use Application\Service\ApplicationConfigService;
use Log\Service\LogService;
use Node\AbstractNode;
use Node\AbstractGateway;
use Node\Entity;
use Magelink\Exception\MagelinkException;
use Magelink\Exception\SyncException;
use Zend\Db\TableGateway\TableGateway;
use Zend\Db\Sql\Where;


class Node extends AbstractNode
{

    /** @var array|NULL $storeViews */
    protected $storeViews = NULL;


    /**
     * @return string $nodeLogPrefix
     */
    protected function getNodeLogPrefix()
    {
        return 'mg2_';
    }

    /**
     * @return bool Whether or not we should enable multi store mode
     */
    public function isMultiStore()
    {
        return (bool) $this->getConfig('multi_store');
    }

    /**
     * Returns an api instance set up for this node. Will return false if that type of API is unavailable.
     * @param string $type The type of API to establish - must be available as a service with the name "magento2_{type}"
     * @return object|false
     */
    public function getApi($type)
    {
        if (!isset($this->_api[$type])) {
            $this->_api[$type] = $this->getServiceLocator()->get('magento2_'.$type);

            $apiExists = $this->_api[$type]->init($this);
            if (!$apiExists) {
                $this->_api[$type] = FALSE;
            }

            $this->getServiceLocator()->get('logService')->log(LogService::LEVEL_INFO,
                $this->getNodeLogPrefix().'init_api', 'Creating API instance '.$type,
                array('type'=>$type, 'api exists'=>$apiExists, 'isset api'=>isset($this->_api[$type]))
            );
        }

        return $this->_api[$type];
    }

    /**
     * Return a data array of all store views
     * @return array $storeViews
     */
    public function getStoreViews()
    {
        if (is_null($this->storeViews)) {
            /** @var \Magento2\Api\RestV1 $restV1 */
            $restV1 = $this->getApi('restV1');
            if (!$restV1) {
                throw new SyncException('Failed to initialize RESTv1 api for store view fetch');
            }else{
                $response = $restV1->get('store/storeViews', array());

                if (count($response) > 0) {
                    $this->storeViews = array();
                    foreach ($response as $storeView) {
                        $this->storeViews[$storeView['id']] = $storeView;
                    }
                    $storeViews = $this->storeViews;
                }else{
                    $storeViews = array();
                }

                $this->getServiceLocator()->get('logService')
                    ->log(LogService::LEVEL_INFO,
                        $this->getNodeLogPrefix().'storeviews',
                        'Loaded store views',
                        array('rest response'=>$response, 'store views'=>$this->storeViews),
                        array('node'=>$this)
                    );
            }
        }else{
            $storeViews = $this->storeViews;
        }

        return $storeViews;
    }

    /**
     * Should set up any initial data structures, connections, and open any required files that the node needs to operate.
     * In the case of any errors that mean a successful sync is unlikely, a Magelink\Exception\InitException MUST be thrown.
     */
    protected function _init()
    {
        $this->getStoreViews();
        $storeCount = count($this->storeViews);

        if ($storeCount == 1 && $this->isMultiStore()) {
            $this->getServiceLocator()->get('logService')
                ->log(LogService::LEVEL_ERROR,
                    $this->getNodeLogPrefix().'mstore_sng',
                    'Multi-store enabled but only one store view!',
                    array(),
                    array('node'=>$this)
                );
        }elseif ($storeCount > 1 && !$this->isMultiStore()) {
            $this->getServiceLocator()->get('logService')
                ->log(LogService::LEVEL_WARN,
                    $this->getNodeLogPrefix().'mstore_miss',
                    'Multi-store disabled but multiple store views!',
                    array(),
                    array('node'=>$this)
                );
        }
    }

    /**
     * This will always be the last call to the Node to close off any open connections, files, etc.
     */
    protected function _deinit() {}

    /**
     * Updates all data into the node’s source - should load and collapse all pending updates and call writeUpdates,
     *   as well as loading and sequencing all actions.
     * @throws NodeException
     */
    public function update()
    {
        $logCode = $this->logTimes('Magento2\Node');

        $startGetActionsTime = time();
        $this->getPendingActions();

        $startGetUpdatesTime = time();
        $this->getPendingUpdates();

        $getEndTime = time();

        $logMessage = 'Magento2\Node update: '.count($this->updates).' updates, '.count($this->actions).' actions.';
        $logEntities = array('node'=>$this, 'actions'=>$this->actions, 'updates'=>$this->updates);
        $this->_logService->log(LogService::LEVEL_DEBUGEXTRA, $logCode.'_list', $logMessage, array(), $logEntities);

        $startProcessActionsTime = time();
        $this->processActions();

        $startProcessUpdatesTime = time();
        $this->processUpdates();
        $endProcessTime = time();

        $getActionsTime = ceil($startGetActionsTime - $startGetUpdatesTime);
        $getUpdatesTime = ceil($getEndTime - $startGetUpdatesTime);
        $processActionsTime = ceil($startProcessActionsTime - $startProcessUpdatesTime);
        $processUpdatesTime = ceil($endProcessTime - $startProcessUpdatesTime);

        $message = 'Get and process actions and updates to Magento2.';
        $logData = array('type'=>'all', 'actions'=>count($this->actions), 'updates'=>count($this->updates),
            'get actions [s]'=>$getActionsTime, 'get updates [s]'=>$getUpdatesTime,
            'process actions [s]'=>$processActionsTime, 'process updates [s]'=>$processUpdatesTime);
        if (count($this->actions) > 0) {
            $logData['per action [s]'] = round(($getActionsTime + $processActionsTime) / count($this->actions), 1);
        }
        if (count($this->updates) > 0) {
            $logData['per update [s]'] = round(($getUpdatesTime + $processUpdatesTime) / count($this->updates), 1);
        }
        $this->getServiceLocator()->get('logService')->log(LogService::LEVEL_INFO, $logCode.'_no', $message, $logData);

        $this->logTimes('Magento2\Node', TRUE);
    }

    /**
     * Implemented in each NodeModule
     * Returns an instance of a subclass of AbstractGateway that can handle the provided entity type.
     *
     * @throws MagelinkException
     * @param string $entityType
     * @return AbstractGateway
     */
    protected function _createGateway($entityType)
    {
        switch ($entityType) {
            case 'customer':
                $gateway = new Gateway\CustomerGateway;
                break;
            case 'product':
                $gateway = new Gateway\ProductGateway;
                break;
            case 'stockitem':
                $gateway = NULL;
                break;
            case 'order':
                $gateway = new Gateway\OrderGateway;
                break;
            // ToDo: Check creditmemo functionality. Stays disabled for now.
            case 'creditmemo':
                $gateway = NULL;
                //$gateway = new Gateway\CreditmemoGateway;
                break;
            default:
                throw new SyncException('Unknown/invalid entity type '.$entityType);
                $gateway = NULL;
        }

        return $gateway;
    }

}
