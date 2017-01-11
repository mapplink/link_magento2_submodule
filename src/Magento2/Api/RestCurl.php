<?php
/**
 * Implements REST access to Magento2
 * @category Magento2
 * @package Magento2\Api
 * @author Andreas Gerhards <andreas@lero9.co.nz>
 * @copyright Copyright (c) 2016 LERO9 Ltd.
 * @license Commercial - All Rights Reserved
 */

namespace Magento2\Api;

use Log\Service\LogService;
use Magelink\Exception\MagelinkException;
use Magelink\Exception\GatewayException;
use Magento2\Node;
use Zend\Http\Client;
use Zend\Http\Headers;
use Zend\Http\Request;
use Zend\Json\Json;
use Zend\ServiceManager\ServiceLocatorAwareInterface;
use Zend\ServiceManager\ServiceLocatorInterface;
use Zend\Stdlib\Parameters;


abstract class RestCurl implements ServiceLocatorAwareInterface
{

    const ERROR_PREFIX = 'REST ERROR: ';

    /** @var ServiceLocatorInterface $this->serviceLocator */
    protected $serviceLocator;
    /** @var Node|NULL $this->node */
    protected $node = NULL;

    /** @var resource|FALSE|NULL $this->curlHandle */
    protected $curlHandle = NULL;
    /** @var string|NULL $this->authorisation */
    protected $authorisation = NULL;
    /** @var Client $this->client */
    protected $client;
    /** @var string|NULL $this->requestType */
    protected $requestType;
    /** @var  Request $this->request */
    protected $request;
    /** @var array $this->curlOptions */
    protected $curlOptions = array();
    /** @var array $this->baseCurlOptions */
    protected $baseCurlOptions = array(
        CURLOPT_RETURNTRANSFER=>TRUE,
        CURLOPT_ENCODING=>'',
        CURLOPT_MAXREDIRS=>10,
        CURLOPT_TIMEOUT=>30,
        CURLOPT_HTTP_VERSION=>CURL_HTTP_VERSION_1_1
    );
    /** @var array $this->clientOptions */
    protected $clientOptions = array(
        'adapter'=>'Zend\Http\Client\Adapter\Curl',
        'curloptions'=>array(CURLOPT_FOLLOWLOCATION=>TRUE),
        'maxredirects'=>0,
        'timeout'=>30
    );


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
     * @param array $headers
     * @return string $initLogCode
     */
    abstract protected function getLogCodePrefix();

    /**
     * @return FALSE|NULL|resource $this->curlHandle
     */
    protected function initCurl(array $headers)
    {
        if (is_null($this->curlHandle)) {
            $this->curlHandle = curl_init();
        }

        $this->curlOptions = array_replace_recursive(
            $this->baseCurlOptions,
            array(CURLOPT_HTTPHEADER=>$this->getHeaders($headers))
        );

        return $this->curlHandle;
    }

    /**
     * @param array $headers
     * @return bool $success
     */
    public function getHeaders(array $headers)
    {
        $cacheControl = 'cache-control: no-cache';

        foreach ($headers as $line) {
            if (strpos($line, strstr($cacheControl, ':', TRUE).':') !== FALSE) {
                $cacheControl = FALSE;
                break;
            }
        }
        if ($cacheControl) {
            $headers[] = $cacheControl;
        }

        return $headers;
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
            // TECHNICAL DEBT // ToDo include parameters
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
            'curl info'=>curl_getinfo($this->curlHandle)
        );

        curl_setopt_array($this->curlHandle, $this->curlOptions);
        unset($this->curlOptions);

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
            // ToDo: Implemented Zend classes and methods
            $headers = array('content-type: application/json');
            $this->initCurl($headers);

            $url = $this->getUrl('integration/admin/token');
            $this->curlOptions[CURLOPT_URL] = $url;

            $this->curlOptions[CURLOPT_CUSTOMREQUEST] = $this->requestType =  'POST';
            $username = $this->node->getConfig('rest_username');
            $password = $this->node->getConfig('rest_password');
            $this->curlOptions[CURLOPT_POSTFIELDS] =  '{"username":"'.$username.'", "password":"'.$password.'"}';

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
        $logCode = $this->getLogCodePrefix().'_call';

        if ($this->authorise()) {
            $this->requestType = strtoupper($httpMethod);
            $setRequestDataMethod = 'set'.ucfirst(strtolower($httpMethod)).'fields';

            $uri = $this->getUrl($callType);
            $headersArray = array(
                'Authorization' => $this->authorisation,
                'Accept'=>'application/json',
                'Content-Type'=>'application/json'
            );

            $headers = new Headers();
            $headers->addHeaders($headersArray);

            $this->request = new Request();
            $this->request->setHeaders($headers);
            $this->request->setUri($uri);
            $this->request->setMethod($this->requestType);

            $this->client = new Client();
            $this->client->setOptions($this->clientOptions);

            $this->$setRequestDataMethod($parameters);
            $response = $this->client->send($this->request);

            if (!is_array($response)) {
                $responseBody = $response->getBody();
                $response = Json::decode($responseBody, Json::TYPE_ARRAY);
            }

            $logData = array('uri'=>$uri, 'headers'=>$headersArray, 'method'=>$this->requestType,
                'options'=>$this->clientOptions, 'parameters'=>$parameters, 'response'=>$response);

            if (is_array($response) && array_key_exists('items', $response)) {
                $response = $response['items'];

            }elseif (isset($response['message'])) {
                if (isset($response['parameters']) && is_array($response['parameters'])) {
                    foreach ($response['parameters'] as $key=>$replace) {
                        $search = '"%'.(++$key).'"';
                        $replace = '`'.$replace.'`';
                        $response['message'] = str_replace($search, $replace, $response['message']);
                    }
                }
                $this->getServiceLocator()->get('logService')
                    ->log(LogService::LEVEL_ERROR, $logCode.'_err', self::ERROR_PREFIX.$response['message'], $logData);
                throw new GatewayException(self::ERROR_PREFIX.$response['message']);
            }
        }else{
            $logData = array();
            $response = NULL;
        }

        $logData['return'] = $response;
        $this->getServiceLocator()->get('logService')
            ->log(LogService::LEVEL_DEBUGEXTRA, $logCode, 'REST call '.$callType, $logData);

        return $response;
    }

    /**
     * @param string $callType
     * @return mixed $curlExecReturn
     */
    public function delete($callType)
    {
        $response = $this->call(Request::METHOD_DELETE, $callType);
        return $response;
    }

    /**
     * @param string $callType
     * @param array $parameters
     * @return mixed $curlExecReturn
     */
    public function get($callType, array $parameters = array())
    {
        $response = $this->call(Request::METHOD_GET, $callType, $parameters);

        return $response;
    }

    /**
     * @param string $callType
     * @param array $parameters
     * @return mixed $curlExecReturn
     */
    public function post($callType, array $parameters = array())
    {
        $response = $this->call(Request::METHOD_POST, $callType, $parameters);
        return $response;
    }

    /**
     * @param string $callType
     * @param array $parameters
     * @return mixed $curlExecReturn
     */
    public function put($callType, array $parameters = array())
    {
        $response = $this->call(Request::METHOD_PUT, $callType, $parameters);
        return $response;
    }

    /**
     * @param string $urlParameters
     * @return bool $success
     */
    protected function setUrlParameters($urlParameters)
    {
        $success = FALSE;

        if (!is_null($this->requestType)) {
            if (isset($urlParameters['filter']) && is_array($urlParameters['filter'])) {
                $parameters = array();

                foreach ($urlParameters['filter'] as $filterKey=>$filter) {
                    foreach ($filter as $key=>$value) {
                        $escapedKey = urlencode($key);
                        $escapedValue = urlencode($value);

                        $logCode = $this->getLogCodePrefix().'_url';
                        $logData = array(
                            'key'=>$key,
                            'escaped key'=>$escapedKey,
                            'value'=>$value,
                            'escaped value'=>$escapedValue,
                            'fields'=>$urlParameters
                        );

                        if ($key != $escapedKey) {
                            $logLevel = LogService::LEVEL_ERROR;
                            $logCode .= '_err';
                            $logMessage = $this->requestType.' field key-value pair is not valid.';
                            throw new MagelinkException($logMessage);

                            $parameters = array();
                            break;
                        }elseif ($value != $escapedValue) {
                            $logLevel = LogService::LEVEL_WARN;
                            $logCode .= '_esc';
                            $logMessage = $this->requestType.' value had to be escaped.';
                            unset($logData['escaped key']);

                            $value = $escapedValue;
                        }else{
                            unset($logLevel);
                        }

                        $parameters['searchCriteria']['filterGroups'][0]['filters'][$filterKey][$key] = $value;

                        if (isset($logLevel)) {
                            $this->getServiceLocator()->get('logService')
                                ->log($logLevel, $logCode, $logMessage, $logData);
                        }
                    }

                    if (count($urlParameters) != 0) {
                        $success = TRUE;
                        $parameterObject = new Parameters($parameters);
                        $success = $this->request->setQuery($parameterObject);
                    }else{

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
        return $this->setUrlParameters($deletefields);
    }

    /**
     * @param array $getfields
     * @return bool $success
     */
    public function setGetfields(array $getfields)
    {
        return $this->setUrlParameters($getfields);
    }

    /**
     * @param array $postfields
     * @return bool $success
     */
    protected function setPostfields(array $postfields)
    {
        $postContent = Json::encode($postfields);
        return $this->request->setContent($postContent);
    }

    /**
     * @param array $putfields
     * @return bool $success
     */
    protected function setPutfields(array $putfields)
    {
        $putContent = Json::encode($putfields);
        return $this->request->setContent($putContent);
    }

    /**
     * @return string $curlError
     */
    public function getError()
    {
        return curl_error($this->curlHandle);
    }

}
