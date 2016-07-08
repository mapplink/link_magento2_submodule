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
use Zend\Json\Json;
use Zend\ServiceManager\ServiceLocatorAwareInterface;
use Zend\ServiceManager\ServiceLocatorInterface;


abstract class RestCurl implements ServiceLocatorAwareInterface
{
    /** @var ServiceLocatorInterface $this->serviceLocator */
    protected $serviceLocator;
    /** @var Node|NULL $this->node */
    protected $node = NULL;

    /** @var resource|FALSE|NULL $this->curlHandle */
    protected $curlHandle = NULL;
    /** @var string|NULL $this->header */
    protected $header = NULL;
    /** @var string $this->authorisation */
    protected $authorisation = NULL;
    /** @var string $this->requestType */
    protected $requestType;
    /** @var array $this->baseCurlOptions */
    protected $baseCurlOptions = array(
        CURLOPT_RETURNTRANSFER=>TRUE,
        CURLOPT_ENCODING=>'',
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
     * @return FALSE|NULL|resource $this->curlHandle
     */
    protected function initCurl()
    {
        $this->getCurlHandle();
        curl_setopt_array($this->curlHandle, $this->baseCurlOptions);

        return $this->curlHandle;
    }

    /**
     * @return FALSE|NULL|resource $this->curlHandle
     */
    protected function getCurlHandle()
    {
        if (is_null($this->curlHandle)) {
            $this->curlHandle = curl_init();
        }

        return $this->curlHandle;
    }

    /**
     * @param string $callType
     * @param array $parameters
     * @return string $url
     */
    protected function getUrl($callType, array $parameters = array())
    {
        $url = trim($this->node->getConfig('web_url'), '/').static::REST_BASE_URI.$callType;
        if (count($parameters) > 0) {
            // @todo include parameters
        }

        return $url;
    }

    /**
     * @param string $callType
     * @return mixed $curlExecResponse
     */
    protected function executeCurl($callType)
    {
        $logData = array(
            'request type'=>$this->requestType,
            'header'=>$this->header,
            'curl info'=>curl_getinfo($this->getCurlHandle())
        );

        $response = curl_exec($this->curlHandle);
        $error = $this->getError();

        $logCode = $this->getLogCodePrefix().'_'.substr(strtolower($this->requestType), 0, 2);
        if ($error) {
            $logCode .= '_cerr';
            $logMessage = $callType.' failed. Curl error: '.$error;
            $logData['curl error'] = $error;
            $this->getServiceLocator()->get('logService')
                ->log(LogService::LEVEL_ERROR, $logCode, $logMessage, $logData);
            $response = NULL;
        }else{
            try {
                $logData['response'] = $response;
                $decodedResponse = Json::decode($response, Json::TYPE_ARRAY);

                $errors = array();
                $errorKeys = array('message', 'parameters', 'trace');

                try{
                    $responseArray = (array) $decodedResponse;
                    foreach ($errorKeys as $key) {

                        if (isset($responseArray[$key])) {
                            if (is_string($responseArray[$key])) {
                                $errors[] = $responseArray[$key];
                            }else{
                                $errors[] = var_export($responseArray[$key], TRUE);
                            }
                        }
                    }
                }catch(\Exception $exception) {
                    $logData['exception'] = $exception->getMessage();
                    foreach ($errorKeys as $key) {
                        if (isset($decodedResponse->$key)) {
                            $errors[] = $decodedResponse->$key;
                        }
                    }
                }

                $error = implode(' ', $errors);
                if (strlen($error) == 0 && current($responseArray) != trim($response, '"')) {
                    $error = 'This does not seem to be a valid '.$callType.' key: '.$response;
                }

                if (strlen($error) == 0) {
                    $response = $responseArray;
                    $logLevel = LogService::LEVEL_INFO;
                    $logCode .= '_suc';
                    $logMessage = $callType.' succeeded. ';
                }else{
                    $logLevel = LogService::LEVEL_ERROR;
                    $logCode .= '_fail';
                    $logMessage = $callType.' failed. Error message: '.$error;
                    $logData['error'] = $error;
                    $response = NULL;
                }

                $this->getServiceLocator()->get('logService')
                    ->log($logLevel, $logCode, $logMessage, $logData);
            }catch (\Exception $exception) {
                $logCode = $logCode.'_err';
                $logMessage = $callType.' failed. Error during decoding of '.var_export($response, TRUE);
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
            unset($this->curlHandle);
        }
    }

    /**
     * @return bool $this->authorisation
     */
    protected function authorise()
    {
        if (is_null($this->authorisation)) {
            $url = $this->getUrl('integration/admin/token');
            $this->setBaseurl($url);

            $header = array('content-type: application/json');
            $this->setHeader($header);

            $username = $this->node->getConfig('rest_username');
            $password = $this->node->getConfig('rest_password');
            $postfields = '{"username":"'.$username.'", "password":"'.$password.'"}';
            $this->setPostfields($postfields);

            $response = $this->executeCurl('Authorisation');

            if (is_null($response)) {
                $this->authorisation = NULL;
            }else{
                $this->authorisation = 'Bearer '.current($response);
            }
        }

        return (bool) $this->authorisation;
    }

    /**
     * @param string $method
     * @param string $callType
     * @param array $parameters
     * @return mixed $response
     */
    protected function call($httpMethod, $callType, array $parameters = array())
    {
        $this->initCurl();
        $this->authorise();

        $header = array('authorisation: '.$this->authorisation, 'content-type: application/json');
        $httpMethod = strtoupper($httpMethod);
        $method = 'set'.ucfirst(strtolower($httpMethod)).'fields';

        $this->initCurl();
        $this->setBaseurl($this->getUrl($callType));
        $this->setHeader($header);
        $this->$method($parameters);

        $response = $this->executeCurl($callType);

        return $response;
    }

    /**
     * @param string $callType
     * @return mixed $curlExecReturn
     */
    public function delete($callType)
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
    public function get($callType, array $parameters = array())
    {
        $response = $this->call('GET', $callType, $parameters);

        if (is_array($response) && array_key_exists('items', $response)) {
            $response = $response['items'];
        }

        return $response;
    }

    /**
     * @param string $callType
     * @param array $parameters
     * @return mixed $curlExecReturn
     */
    public function post($callType, array $parameters = array())
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
    public function put($callType, array $parameters = array())
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

        $this->header = $header;

        return curl_setopt($this->getCurlHandle(), CURLOPT_HTTPHEADER, $header);
    }

    /**
     * @param string $url
     * @return bool $success
     */
    public function setBaseurl($url)
    {
var_dump($url);
        return curl_setopt($this->getCurlHandle(), CURLOPT_URL, $url);
    }

    /**
     * @param string $urlParameters
     * @return bool $success
     */
    protected function setUrlParameters($urlParameters)
    {
        $success = FALSE;

        $url = curl_getinfo($this->getCurlHandle(), CURLINFO_EFFECTIVE_URL);
        if (!is_null($this->requestType)) {
            if (isset($urlParameters['filter']) && is_array($urlParameters['filter'])) {
                $parameters = '';
                $concatCharacter = '?';

                foreach ($urlParameters['filter'] as $filterKey=>$filter) {
                    foreach ($filter as $key=>$value) {
                        $escapedKey = urlencode($key);
                        $escapedValue = urlencode($value);

                        if ($key != $escapedKey || $value != $escapedValue) {
                            $logCode = $this->getLogCodePrefix().'_url__err';
                            $logMessage = $this->requestType.' field key-value pair is not valid.';
                            $logData = array(
                                'key'=>$key,
                                'escaped key'=>$escapedKey,
                                'value'=>$value,
                                'escaped value'=>$escapedValue,
                                'fields'=>$urlParameters
                            );
                            $this->getServiceLocator()->get('logService')
                                ->log(LogService::LEVEL_ERROR, $logCode, $logMessage, $logData);
                            /** @todo */
                            throw new MagelinkException($logMessage);
                            $parameters = array();
                            break;
                        }else {
                            $parameters .= $concatCharacter.htmlentities('searchCriteria[filterGroups][0]'
                                .'[filters]['.$filterKey.']['.$key.']='.$value);
                        }
                        $concatCharacter = '&';
                    }

                    if (count($urlParameters) != 0) {
                        $success = $this->setBaseurl($url.$parameters);
                        $success &= curl_setopt($this->getCurlHandle(), CURLOPT_CUSTOMREQUEST, $this->requestType);
                        $success &= curl_setopt($this->getCurlHandle(), CURLOPT_POSTFIELDS, '');
                    }
                }
            }else{
                $success = TRUE;
            }
        }

        return (bool) $success;
    }

    /**
     * @param string $postfields
     * @return bool $success
     */
    public function setDeletefields($deletefields)
    {
        $this->requestType = 'DELETE';
        $success = $this->setUrlParameters($deletefields);

        return (bool) $success;
    }

    /**
     * @param array $getfields
     * @return bool $success
     */
    public function setGetfields(array $getfields)
    {
        $this->requestType = 'GET';
        $success = $this->setUrlParameters($getfields);

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
        $success = curl_setopt($this->getCurlHandle(), CURLOPT_CUSTOMREQUEST, $this->requestType);
        $success &= curl_setopt($this->getCurlHandle(), CURLOPT_POSTFIELDS, $bodyfields);

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
