<?php
/**
 * Implements REST access to Magento2
 * @category Magento2
 * @package Magento2\Api
 * @author Andreas Gerhards <andreas@lero9.co.nz>
 * @copyright Copyright (c) 2016 LERO9 Ltd.
 * @license http://opensource.org/licenses/BSD-3-Clause BSD-3-Clause - Please view LICENSE.md for more information
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
