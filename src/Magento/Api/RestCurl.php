<?php
/**
 * Implements SOAP access to Magento
 * @category Magento
 * @package Magento\Api
 * @author Matt Johnston
 * @author Andreas Gerhards <andreas@lero9.co.nz>
 * @copyright Copyright (c) 2014 LERO9 Ltd.
 * @license Commercial - All Rights Reserved
 */

namespace Magento2\Api;

use Log\Service\LogService;
use Magelink\Exception\MagelinkException;
use Magento2\Node;
use Zend\Escaper\Escaper;
use Zend\ServiceManager\ServiceLocatorAwareInterface;
use Zend\ServiceManager\ServiceLocatorInterface;


abstract class RestCurl implements ServiceLocatorAwareInterface
{

    /** @var ServiceLocatorInterface $this->serviceLocator */
    protected $serviceLocator;
    /** @var Node|NULL $this->node */
    protected $node = NULL;

    /** @var resource|FALSE|NULL $this->curlHandle */
    protected $curlHandle;
    /** @var string $this->authorisation */
    protected $authorisation;
    /** @var string $this->requestType */
    protected $requestType;
    /** @var array $this->curlOptions */
    protected $curlOptions = array(
        CURLOPT_RETURNTRANSFER=>TRUE,
        CURLOPT_ENCODING=>"",
        CURLOPT_MAXREDIRS=>10,
        CURLOPT_TIMEOUT=>30,
        CURLOPT_HTTP_VERSION=>CURL_HTTP_VERSION_1_1
    );

    /**
     * Rest constructor
     */
    public function __construct()
    {
        $this->initCurl();
    }

    /**
     * Rest destructor
     */
    public function __destruct()
    {
        $this->closeCurl();
    }

    /**
     * @return ServiceLocatorInterface
     */
    public function getServiceLocator()
    {
        return $this->serviceLocator;
    }

    /**
     * @param ServiceLocatorInterface $serviceLocator
     */
    public function setServiceLocator(ServiceLocatorInterface $serviceLocator)
    {
        $this->serviceLocator = $serviceLocator;
    }

    /**
     * @param Node $magentoNode
     * @return bool $success
     */
    public function init(Node $magentoNode)
    {
        $this->node = $magentoNode;
        return (bool) $this->curlHandle();
    }

    /**
     * @return string $initLogCode
     */
    abstract protected function getLogCodePrefix();

    /**
     * @return FALSE|NULL|resource $this->curlHandle
     */
    protected function initCurl()
    {
        if (!$this->curlHandle) {
            $this->curlHandle = curl_init();
            curl_setopt_array($this->curlHandle, $this->curlOptions);
        }

        return $this->curlHandle;
    }

    /**
     * @return void
     */
    protected function closeCurl()
    {
        if (!is_null($this->curlHandle)) {
            curl_close($this->curlHandle);
            unset($this->curlHandle);
        }
    }

    public function authorise()
    {
        $url = $this->node->getConfig('web_url').'/index.php/rest/V1/integration/admin/token';
        $this->setBaseurl($url);

        $header = array('content-type: application/json');
        $this->setHeader($header);

        $username = $this->node->getConfig('rest_username');
        $password = $this->node->getConfig('rest_password');
        $postfields = '{"username":"'.$username.'","password":"'.$password.'"}';
        $this->setPostfields($postfields);

        $response = $this->call();
        $error = $this->getError();

        $logCode = $this->getLogCodePrefix();
        if ($error) {
            $logCode .= '_au_cerr';
            $logMessage = 'Authorisation failed. Curl error: '.$error;
            $logData = array('curl error'=>$error);
            $this->getServiceLocator()->get('logService')
                ->log(LogService::LEVEL_ERROR, $logCode, $logMessage, $logData);
            $this->authorisation = NULL;
        }else{
            try {
                $logData = array('response'=>$response);
                $responseArray = json_decode($response);

                $error = array();
                if (array_key_exists('message', $responseArray)) {
                    $error[] = $responseArray['message'];
                }
                if (array_key_exists('trace', $responseArray)) {
                    $error[] = $responseArray['trace'];
                }
                $error = implode(' ', $error);
                if (strlen($error) == 0 && current($responseArray) != trim($response, '"')) {
                    $error = 'This does not seem to be a valid authorisation key: '.$response;
                }

                if (strlen($error) == 0) {
                    $this->authorisation = 'Bearer '.$response;
                    $logLevel = LogService::LEVEL_INFO;
                    $logCode = $this->getLogCodePrefix().'_au_suc';
                    $logMessage = 'Authorisation suceed. Authorisation code: '.$this->authorisation;
                }else{
                    $logLevel = LogService::LEVEL_ERROR;
                    $logCode = $this->getLogCodePrefix().'_au_fail';
                    $logMessage = 'Authorisation failed. Error message: '.$error;
                    $this->authorisation = NULL;
                }

                $this->getServiceLocator()->get('logService')
                    ->log($logLevel, $logCode, $logMessage, $logData);
            }catch (\Exception $exception) {
                $logCode = $this->getLogCodePrefix().'_au_err';
                $logMessage = 'Authorisation failed. Error during decoding of '.var_export($response, TRUE);
                $this->getServiceLocator()->get('logService')
                    ->log(LogService::LEVEL_ERROR, $logCode, $logMessage, $logData);
                $this->authorisation = NULL;
            }
        }

        return $this->authorisation;
    }
    /**
     * @param array $header
     * @return bool $success
     */
    public function setHeader(array $header)
    {
        $cacheControl = 'cache-control: no-cache';

        foreach ($header as $line) {
            if (strpos($line, strstr($cacheControl, ':').':') !== FALSE) {
                $cacheControl = FALSE;
                break;
            }
        }
        if ($cacheControl) {
            $header[] = $cacheControl;
        }

        return curl_setopt($this->initCurl(), CURLOPT_HTTPHEADER, $header);
    }

    /**
     * @param string $url
     * @return bool $success
     */
    public function setBaseurl($url)
    {
        return curl_setopt($this->initCurl(), CURLOPT_URL, $url);
    }

    /**
     * @param array $getfields
     * @return bool $success
     */
    public function setGetfields(array $getfields)
    {
        $url = curl_getinfo($this->initCurl(), CURLINFO_EFFECTIVE_URL);
        if (is_null($this->requestType)) {
            $this->requestType = 'GET';

            $gets = array();
            $getUrl = $url.'?';
            $escaper = new Escaper('utf-8');

            foreach ($getfields as $key=>$value) {
                $escapedKey = $escaper->escapeUrl($key);

                if ($key != $escapedKey) {
                    $logCode = $this->getLogCodePrefix().'_get__err';
                    $logMessage = 'GET field key '.var_export($key, TRUE).' is not valid.';
                    $logData = array('key'=>$key, 'escapedKey'=>$escapedKey, 'value'=>$value, 'getfields'=>$getfields);
                    $this->getServiceLocator()->get('logService')
                        ->log(LogService::LEVEL_ERROR, $logCode, $logMessage, $logData);
/** ToDo */
throw new MagelinkException($logMessage);
                    $gets = array();
                    break;
                }else{
                    $escapedValue = $escaper->escapeUrl($value);
                    $gets[] = $key.'='.$escapedValue;
                }
            }

            if (count($gets) == 0) {
                $success = FALSE;
            }else{
                $getUrl .= implode('&', $gets);
                $success = $this->setBaseurl($getUrl);
                $success &= curl_setopt($this->initCurl(), CURLOPT_CUSTOMREQUEST, $this->requestType);
            }
        }else{
            $success = FALSE;
        }

        return (bool) $success;
    }

    /**
     * @param string $postfields
     * @return bool $success
     */
    public function setPostfields($postfields)
    {
        if (is_null($this->requestType)) {
            $this->requestType = 'POST';
            $success = curl_setopt($this->initCurl(), CURLOPT_CUSTOMREQUEST, $this->requestType);
            $success &= curl_setopt($this->initCurl(), CURLOPT_CUSTOMREQUEST, $postfields);
        }else{
            $success = FALSE;
        }

        return (bool) $success;
    }

    /**
     * @return mixed $curlExecReturn
     */
    public function call()
    {
        return curl_exec($this->curlHandle);
    }

    /**
     * @return string $curlError
     */
    public function getError()
    {
        return curl_error($this->curlHandle);
    }

}
