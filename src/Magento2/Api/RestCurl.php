<?php
/**
 * Implements SOAP access to Magento2
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
    protected $authorisation = NULL;
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
     * @param Node $magento2Node
     * @return bool $success
     */
    public function init(Node $magento2Node)
    {
        $this->node = $magento2Node;
        return (bool) $this->node;
    }

    /**
     * @return string $initLogCode
     */
    abstract protected function getLogCodePrefix();

    /**
     * @param string $callType
     * @param array $parameters
     * @return string $url
     */
    protected function getUrl($callType, array $parameters = array())
    {
        $url = $this->node->getConfig('web_url').static::REST_BASE_URI.$callType;
        if (count($parameters) > 0) {
            // @todo include parameters
        }

        return $url;
    }

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
     * @param string $callType
     * @return mixed $curlExecReturn
     */
    protected function executeCurl($callType)
    {
        $response = curl_exec($this->curlHandle);
        $error = $this->getError();

        $callType = strtolower($callType);
        $logCode = $this->getLogCodePrefix().'_'.substr($callType, 0, 2);
        if ($error) {
            $logCode .= '_cerr';
            $logMessage = ucfirst($callType).' failed. Curl error: '.$error;
            $logData = array('curl error'=>$error);
            $this->getServiceLocator()->get('logService')
                ->log(LogService::LEVEL_ERROR, $logCode, $logMessage, $logData);
            $response = NULL;
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
                    $error = 'This does not seem to be a valid '.$callType.' key: '.$response;
                }

                if (strlen($error) == 0) {
                    $response = $responseArray;
                    $logLevel = LogService::LEVEL_INFO;
                    $logCode .= '_suc';
                    $logMessage = ucfirst($callType).' suceed. ';
                }else{
                    $logLevel = LogService::LEVEL_ERROR;
                    $logCode .= '_fail';
                    $logMessage = ucfirst($callType).' failed. Error message: '.$error;
                    $response = NULL;
                }

                $this->getServiceLocator()->get('logService')
                    ->log($logLevel, $logCode, $logMessage, $logData);
            }catch (\Exception $exception) {
                $logCode = $logCode.'_err';
                $logMessage = 'Authorisation failed. Error during decoding of '.var_export($response, TRUE);
                $this->getServiceLocator()->get('logService')
                    ->log(LogService::LEVEL_ERROR, $logCode, $logMessage, $logData);
                $response = NULL;
            }
        }

        return $response;
    }

    /**
     * @return void
     */
    protected function closeCurl()
    {
        if (!is_null($this->curlHandle)) {
            curl_close($this->curlHandle);
            unset($this->curlHandle, $this->authorisation);
        }
    }

    /**
     * @return null|string $this->authorisation
     */
    protected function authorise()
    {
        $this->initCurl();

        if (is_null($this->authorisation)) {
            $url = $this->getUrl('integration/admin/token');
            $this->setBaseurl($url);

            $header = array('content-type: application/json');
            $this->setHeader($header);

            $username = $this->node->getConfig('rest_username');
            $password = $this->node->getConfig('rest_password');
            $postfields = '{"username":"'.$username.'","password":"'.$password.'"}';
            $this->setPostfields($postfields);

            $response = $this->executeCurl('Authorisation');

            if (is_null($response)) {
                $this->authorisation = NULL;
            }else{
                $this->authorisation = 'Bearer '.current($response);
            }
        }

        return $this->authorisation;
    }

    /**
     * @param string $method
     * @param string $callType
     * @param array $parameters
     * @return mixed $curlExecReturn
     */
    protected function call($httpMethod, $callType, array $parameters = array())
    {
        $httpMethod = strtoupper($httpMethod);
        $method = 'set'.ucfirst(strtolower($httpMethod)).'fields';

        $this->initCurl();
        $this->authorise();

        $header = array(
            'Authorisation: '.$this->authorisation,
            'content-type: application/json'
        );

        $this->setHeader($header);
        $this->$method($parameters);

        $response = $this->executeCurl('call');

        return $response;
    }

    /**
     * @param string $callType
     * @return mixed $curlExecReturn
     */
    public function deleteCall($callType)
    {
        $response = $this->call('DELETE', $callType);
        // @todo
        return $response;
    }

    /**
     * @param string $callType
     * @param array $parameters
     * @return mixed $curlExecReturn
     */
    public function getCall($callType, array $parameters = array())
    {
        $response = $this->call('GET', $callType, $parameters);
        return $response['items'];
    }

    /**
     * @param string $callType
     * @param array $parameters
     * @return mixed $curlExecReturn
     */
    public function postCall($callType, array $parameters = array())
    {
        $response = $this->call('POST', $callType, $parameters);
        // @todo
        return $response;
    }

    /**
     * @param string $callType
     * @param array $parameters
     * @return mixed $curlExecReturn
     */
    public function putCall($callType, array $parameters = array())
    {
        $response = $this->call('PUT', $callType, $parameters);
        // @todo
        return $response;
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
     * @param string $urlfields
     * @return bool $success
     */
    public function setUrlfields($urlfields)
    {
        $url = curl_getinfo($this->initCurl(), CURLINFO_EFFECTIVE_URL);
        if (is_null($this->requestType)) {
            $this->requestType = 'DELETE';

            $urlParameters = array();
            $url = $url.'?';
            $escaper = new Escaper('utf-8');

            foreach ($urlfields as $key=>$value) {
                $escapedKey = $escaper->escapeUrl($key);

                if ($key != $escapedKey) {
                    $logCode = $this->getLogCodePrefix().'_del__err';
                    $logMessage = $this->requestType.' field key '.var_export($key, TRUE).' is not valid.';
                    $logData = array('key'=>$key, 'escapedKey'=>$escapedKey, 'value'=>$value, 'fields'=>$urlfields);
                    $this->getServiceLocator()->get('logService')
                        ->log(LogService::LEVEL_ERROR, $logCode, $logMessage, $logData);
                    /** @todo */
                    throw new MagelinkException($logMessage);
                    $urlParameters = array();
                    break;
                }else{
                    $escapedValue = $escaper->escapeUrl($value);
                    $urlParameters[] = $key.'='.$escapedValue;
                }
            }

            if (count($urlParameters) == 0) {
                $success = FALSE;
            }else{
                $url .= implode('&', $urlParameters);
                $success = $this->setBaseurl($url);
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
    public function setDeletefields($deletefields)
    {
        $url = curl_getinfo($this->initCurl(), CURLINFO_EFFECTIVE_URL);
        if (is_null($this->requestType)) {
            $this->requestType = 'DELETE';
            $success = $this->setUrlfields($deletefields);
        }else{
            $success = FALSE;
        }

        return (bool) $success;
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
            $success = $this->setUrlfields($getfields);
        }else{
            $success = FALSE;
        }

        return (bool) $success;
    }

    /**
     * @param string $requestType
     * @param string $bodyfields
     * @return bool $success
     */
    protected function setBodyfields($requestType, $bodyfields)
    {
        $this->requestType = $requestType;
        $success = curl_setopt($this->initCurl(), CURLOPT_CUSTOMREQUEST, $this->requestType);
        $success &= curl_setopt($this->initCurl(), CURLOPT_CUSTOMREQUEST, $bodyfields);

        return (bool) $success;
    }

    /**
     * @param string $postfields
     * @return bool $success
     */
    protected function setPostfields($postfields)
    {
        return $this->setBodyfields('POST', $postfields);
    }

    /**
     * @param string $putfields
     * @return bool $success
     */
    protected function setPutfields($putfields)
    {
        return $this->setBodyfields('PUT', $putfields);
    }

    /**
     * @return string $curlError
     */
    public function getError()
    {
        return curl_error($this->curlHandle);
    }

}
