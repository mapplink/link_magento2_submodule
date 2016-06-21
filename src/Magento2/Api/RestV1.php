<?php
/**
 * Implements SOAP access to Magento2
 * @category Magento2
 * @package Magento2\Api
 * @author Matt Johnston
 * @author Andreas Gerhards <andreas@lero9.co.nz>
 * @copyright Copyright (c) 2014 LERO9 Ltd.
 * @license Commercial - All Rights Reserved
 */

namespace Magento2\Api;

use Log\Service\LogService;
use Magelink\Exception\MagelinkException;


class RestV1 extends RestCurl
{
    const REST_BASE_URI = '/index.php/rest/V1/';

    /**
     * @return string $initLogCode
     */
    protected function getLogCodePrefix()
    {
        return 'mg2_rest';
    }

}
