<?php

$moduleConfig = array (
    'node_types'=>array(
        'magento2'=>array(
            'module'=>'Magento2', // Module name used for this node
            'name'=>'Magento 2', // Human-readable node name
            'store_specific'=>FALSE, // TRUE if this node only operates with one store view, FALSE if on all at once
            'entity_type_support'=>array( // List of entity type codes that this module supports
                'product',
                'stockitem',
                'customer',
                'order',
                'creditmemo',
                #'address',
                #'orderitem',
            ),
            'config'=>array( // Config options to be displayed to the administrator
                'enterprise'=>array(
                    'label'=>'Enterprise Edition? (DO NOT CHANGE)',
                    'type'=>'Checkbox',
                    'default'=>FALSE
                ),
                'multi_store'=>array(
                    'label'=>'Enable Multi-Store support? (DO NOT CHANGE)',
                    'type'=>'Checkbox',
                    'default'=>TRUE
                ),
                'web_url'=>array(
                    'label'=>'Base Web URL',
                    'type'=>'Text',
                    'required'=>TRUE
                ),

                'rest_username'=>array(
                    'label'=>'REST Username',
                    'type'=>'Text',
                    'required'=>TRUE
                ),
                'rest_password'=>array(
                    'label'=>'REST Password',
                    'type'=>'Text',
                    'required'=>TRUE
                ),
                'consumer_key'=>array(
                    'label'=>'Consumer Key',
                    'type'=>'Text',
                    'required'=>FALSE
                ),
                'consumer_secret'=>array(
                    'label'=>'Consumer Secret',
                    'type'=>'Text',
                    'required'=>FALSE
                ),
                'access_token'=>array(
                    'label'=>'Access Token',
                    'type'=>'Text',
                    'required'=>FALSE
                ),
                'access_secret'=>array(
                    'label'=>'Access Token Secret',
                    'type'=>'Text',
                    'required'=>FALSE
                ),

                'db_hostname'=>array(
                    'label'=>'Database Host',
                    'type'=>'Text',
                    'required'=>FALSE
                ),
                'db_schema'=>array(
                    'label'=>'Database Schema',
                    'type'=>'Text',
                    'required'=>FALSE
                ),
                'db_username'=>array(
                    'label'=>'Database Username',
                    'type'=>'Text',
                    'required'=>FALSE
                ),
                'db_password'=>array(
                    'label'=>'Database Password',
                    'type'=>'Text',
                    'required'=>FALSE
                ),

                'load_stock'=>array(
                    'label'=>'Load stock data? (SLOW)',
                    'type'=>'Checkbox',
                    'default'=>FALSE
                ),
                'load_full_customer'=>array(
                    'label'=>'Load full customer data?',
                    'type'=>'Checkbox',
                    'default'=>FALSE
                ),
                'load_full_order'=>array(
                    'label'=>'Load full order data?',
                    'type'=>'Checkbox',
                    'default'=>FALSE
                ),

                'product_attributes'=>array(
                    'label'=>'Extra product attributes to load',
                    'type'=>'Text',
                    'default'=>array()
                ),
                'customer_attributes'=>array(
                    'label'=>'Extra customer attributes to load',
                    'type'=>'Text',
                    'default'=>array()
                ),
                /*'customer_special_attributes'=>array(
                    'label'=>'Extra customer attribute (stored in taxvat)',
                    'type'=>'Text',
                    'default'=>''
                ),*/
                'time_delta_customer'=>array(
                    'label'=>'CUSTOMER API : timezone delta in hours',
                    'type'=>'Text',
                    'default'=>'0'
                ),
                'time_delta_product'=>array(
                    'label'=>'PRODUCT API : timezone delta in hours',
                    'type'=>'Text',
                    'default'=>'0'
                ),
                'time_delta_order'=>array(
                    'label'=>'ORDER API : timezone delta in hours',
                    'type'=>'Text',
                    'default'=>'0'
                ),
                'time_correction_order'=>array(
                    'label'=>'ORDER : time correction in hours on import into HOPS',
                    'type'=>'Text',
                    'default'=>'0'
                ),
                'time_delta_creditmemo'=>array(
                    'label'=>'CREDIT MEMO fetch : timezone delta in hours',
                    'type'=>'Text',
                    'default'=>'0'
                ),
                'api_overlapping_seconds'=>array(
                    'label'=>'API calls : overlapping seconds to avoid missing information',
                    'type'=>'Text',
                    'default'=>'12'
                )
            ),
        )
    ),
    'controllers'=>array(
        'invokables'=>array(
            'Magento2\Controller\Console'=>'Magento2\Controller\Console',
        ),
    ),
    'service_manager'=>array(
        'invokables'=>array(
            'magento2_restV1'=>'Magento2\Api\RestV1',
            'magento2_db'=>'Magento2\Api\Db',
            'magento2Service'=>'Magento2\Service\Magento2Service',
            'magento2ConfigService'=>'Magento2\Service\Magento2ConfigService',
            //'transform_order_total'=>'Magento2\Transform\OrderTotalTransform'
        ),
        'shared'=>array(
            'magento2_restV1'=>FALSE,
            'magento2_db'=>FALSE
        )
    ),
    'console'=>array(
        'router'=>array(
            'routes'=>array(
                'magento2-console'=>array(
                    'options'=>array(
                        'route'=>'magento2 <task> <id> [<params>]',
                        'defaults'=>array(
                            'controller'=>'Magento2\Controller\Console',
                            'action'=>'run'
                        )
                    )
                )
            )
        )
    )
);

return $moduleConfig;
