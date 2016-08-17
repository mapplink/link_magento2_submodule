<?php
/**
 * Magento2\Gateway\OrderGateway
 * @category Magento2
 * @package Magento2\Gateway
 * @author Matt Johnston
 * @author Andreas Gerhards <andreas@lero9.co.nz>
 * @copyright Copyright(c) 2014 LERO9 Ltd.
 * @license Commercial - All Rights Reserved
 */

namespace Magento2\Gateway;

use Entity\Entity;
use Entity\Update;
use Entity\Action;
use Entity\Wrapper\Product;
use Magento2\Service\Magento2Service;
use Log\Service\LogService;
use Magelink\Exception\MagelinkException;
use Magelink\Exception\SyncException;
use Magelink\Exception\NodeException;
use Magelink\Exception\GatewayException;


class ProductGateway extends AbstractGateway
{

    const GATEWAY_ENTITY = 'product';
    const GATEWAY_ENTITY_CODE = 'p';

    /** @var Magento2Service $this->magento2Service */
    protected $magento2Service = NULL;
    /** @var array $this->attributeSets */
    protected $attributeSets;


    // ToDo: Move mapping to config
    /** @var array self::$colourById */
    protected static $colourById = array(93=>"Alabaster", 94=>"Am G/Gran", 95=>"Am Gr/Blk", 96=>"Am Grn", 97=>"Am/Car",
        98=>"Am/Des", 99=>"Aniseed", 100=>"Army", 101=>"Army Gran", 102=>"Army Green", 103=>"Ash", 104=>"Beige", 105=>"Black",
        106=>"Black Croc", 107=>"Black Magic", 108=>"Black Marle", 109=>"Black Pony", 110=>"Black Sheep", 111=>"Black Twill",
        112=>"Black/Black", 113=>"Black/Blue", 114=>"Black/Brass", 115=>"Black/Burg.Gingham", 116=>"Black/Burgundy",
        117=>"Black/Char.Lurex", 118=>"Black/Charcoal", 119=>"Black/Check", 120=>"Black/Dark", 121=>"Black/Fluoro",
        122=>"Black/Glow", 123=>"Black/Gold", 124=>"Black/Green", 125=>"Black/Grey", 126=>"Black/Ink", 127=>"Black/Light",
        128=>"Black/Multi", 129=>"Black/Org", 130=>"Black/Pearl", 131=>"Black/Pink", 132=>"Black/Pitch",
        133=>"Black/Pitch Tartan", 134=>"Black/Plum", 135=>"Black/Pop", 136=>"Black/Poppy", 137=>"Black/Purple", 138=>"Black/Red",
        139=>"Black/Silver", 140=>"Black/Tartan", 141=>"Black/Violet", 142=>"Black/White", 143=>"Black/Yellow", 144=>"Blackball",
        145=>"Blackboard", 146=>"Blackcheck", 147=>"Blacken", 148=>"Blackfelt", 149=>"Blackfelt/Leather", 150=>"Blackout",
        151=>"Blackpool", 152=>"Blackstretch", 153=>"Blacksuit", 154=>"Blackwash", 155=>"Blackwax", 156=>"BlackYellow",
        157=>"Bleach", 158=>"Blk/Rattlesnake", 159=>"Blood Stone", 160=>"Blu Chk", 161=>"Blue", 162=>"Blue Logo", 163=>"Blue Mix",
        164=>"Blue Tartan", 165=>"Bluegum/Licorice", 166=>"Bluescreen", 167=>"Blush", 168=>"Bonded", 169=>"Brass",
        170=>"Brass/Black", 171=>"Brass/Brass", 172=>"Brick", 173=>"Bronze", 174=>"Brown", 175=>"Brown Logo", 176=>"Brown Multi",
        177=>"Brown Snake", 178=>"Brown Tartan", 179=>"Brownie", 180=>"Buff", 181=>"Burg.Ging/Espresso", 182=>"Burgundy",
        183=>"Burgundy Gingham", 184=>"Burgundy/Black", 185=>"Burgundy/Espresso", 186=>"Burgundy/Gingham", 187=>"Camo",
        188=>"Candy", 189=>"Carbon", 190=>"Cargo Green", 191=>"Caviar", 192=>"Char Gran", 193=>"Char Tri", 194=>"Char/Utility",
        195=>"Charcoal", 196=>"Cherry", 197=>"Chilli", 198=>"Coffee", 199=>"Coral", 200=>"Cream", 201=>"Crystal Black C",
        202=>"Crystal White C", 203=>"Crystal/Pcorn", 204=>"Crystal/Walnut", 205=>"Dark Blue", 206=>"Dark Brown",
        207=>"Dark Dust", 208=>"Dark Tweed", 209=>"Darl Indigo", 210=>"Dash/Black", 211=>"Delft", 212=>"Desert",
        213=>"Desert Mix", 214=>"Desert PJ", 215=>"Desert Utility Mix", 216=>"Dusk", 217=>"Ebony", 218=>"Ecru", 219=>"Electric",
        220=>"Flesh", 221=>"Flint", 222=>"Floral", 223=>"Forest", 224=>"Fuchsia/Pop", 225=>"Fudge", 226=>"Glow", 227=>"Gold",
        228=>"Green", 229=>"Green Mix", 230=>"Green/Grey", 231=>"Grey", 232=>"Grey Marle", 233=>"Grey/Blue", 234=>"Grey/Ink",
        235=>"Grey/Navy", 236=>"Grey/Pink", 237=>"Grey/Purple", 238=>"Hands/Black", 239=>"Homemade Black", 240=>"Ice",
        241=>"Indigo", 242=>"Ink", 243=>"Ink/Black", 244=>"Iron", 245=>"Jade", 246=>"Jetblack", 247=>"Kelp", 248=>"Khaki",
        249=>"Kidblack", 250=>"Lapis", 251=>"Lateshow", 252=>"Lavender", 253=>"Light Grey", 254=>"Light Mix", 255=>"Lime",
        256=>"Logo", 257=>"Mad Wax", 258=>"Magenta", 259=>"Mahogany", 260=>"Maroon", 261=>"Matt Black", 262=>"Matt Grey",
        263=>"Melon", 264=>"Metal", 265=>"Midnight", 266=>"Midnight/Black", 267=>"Military", 268=>"Milk", 269=>"Mixed Chk",
        270=>"Monster", 271=>"Multi", 272=>"Mushroom", 273=>"Mustard", 274=>"N/A", 275=>"Navy", 276=>"Navy Check",
        277=>"Navy/Black", 278=>"Navy/Ivory", 279=>"Navy/White", 280=>"Nickel", 281=>"Noir", 282=>"Nori", 283=>"Olive",
        284=>"Onyx", 285=>"Orange", 286=>"Orange Logo", 287=>"Orange Pony", 288=>"Oyster", 289=>"P/M/Pew", 290=>"P/M/Pink",
        291=>"Paint/Black", 292=>"Papaya", 293=>"Passport", 294=>"Pearl", 295=>"Peat", 296=>"Peat/Black", 297=>"Petrol",
        298=>"Petrol/Black", 299=>"Petrol/Charcoal", 300=>"Pew/Pk", 301=>"Pewt/Gran", 302=>"Pewt/Pewt", 303=>"Pewt/Tri",
        304=>"Pewter", 305=>"Pewter/Pewter", 306=>"Pewter/Tri", 307=>"Pink", 308=>"Pink Mix", 309=>"Pink/Tri", 310=>"Pirate",
        311=>"Pitch", 312=>"Pitch Tartan", 313=>"Pitch Tartan/Black", 314=>"Pitch/Black", 315=>"Pitch/Tartan", 316=>"PJ Print",
        317=>"Pk/Flor", 318=>"Pk/Pew", 319=>"Plum", 320=>"Plum/Black", 321=>"Plum/Espresso", 322=>"Plum/Gingham",
        323=>"Pop/White", 324=>"Poppy", 325=>"Potion", 326=>"Print", 327=>"Print Mix", 328=>"Pumice", 329=>"Purple",
        330=>"Quartz", 331=>"Raven", 332=>"Red", 333=>"Red Multi", 334=>"Red Rose", 335=>"Red/Black", 336=>"Red/White",
        337=>"Rose Red", 338=>"Rosewood", 339=>"Royal", 340=>"Royal Pony", 341=>"Safari", 342=>"Sapphire", 343=>"Sateen",
        344=>"Satellite", 345=>"Scarlet", 346=>"Scuba", 347=>"Shadow", 348=>"Silver", 349=>"Silver Marle", 350=>"Silver/ Gold",
        351=>"Skeleton", 352=>"Sky Pony", 353=>"Smoke", 354=>"Smoke/Black", 355=>"Smoke/Green", 356=>"Soap", 357=>"Steel",
        358=>"Steel/Black", 359=>"Steel/Sil", 360=>"String", 361=>"Stripe/Black", 362=>"T-Shell", 363=>"Tar", 364=>"Tartan",
        365=>"Thunder", 366=>"Tidal", 367=>"Tortoise", 368=>"Truffle", 369=>"Tweed", 370=>"U/Pewt", 371=>"Utility Green",
        372=>"Utility Grn", 373=>"Utility/Pk", 374=>"Vamp", 375=>"Vintage Black", 376=>"White", 377=>"White/Black",
        378=>"White/Blue", 379=>"White/Green", 380=>"White/Multi", 381=>"White/Navy", 382=>"White/Red", 383=>"Whitewash",
        384=>"Yellow", 385=>"Zambesi Black", 386=>"Black Diamond", 387=>"Black Diamond/Blk", 388=>"Black/Pumice",
        389=>"Bottle Prism", 390=>"Bottle Prism/Black", 391=>"Burgundy/Dusk", 392=>"Burgundy/Red", 393=>"Diamond Mix Print",
        394=>"Dusk/Storm", 395=>"Green Diamond", 396=>"Green/Green", 397=>"Green/Storm", 398=>"Orange/Pumice", 399=>"Orange/Red",
        400=>"Prism Mix Print", 401=>"Red Prism", 402=>"Red/Orange", 403=>"Storm", 404=>"Navy/Bleach", 405=>"Navy/Yellow",
        406=>"Plaid", 407=>"Grey/Black", 408=>"Grey/Burgundy", 409=>"Stripe", 410=>"Black Russian", 411=>"Blood", 412=>"Tear",
        413=>"Blk Rhodium", 414=>"Oxidised Silver", 415=>"9ct Gold", 416=>"Glass/Silver", 417=>"Resin/Petals",
        418=>"Sterling Silver", 419=>"Blue/White", 420=>"Black/Print", 421=>"Blue Slate", 422=>"Blue Slate/Black",
        423=>"Charcoal Marle", 424=>"Indigo/Black", 425=>"Inkpen", 426=>"Inkpen/Black", 427=>"Licorice", 428=>"Licorice/Black",
        429=>"Licorice/Steel", 430=>"Oil", 431=>"Stone", 432=>"Stone/Black", 433=>"Navy Stripe", 434=>"Ashphalt/Tarseal",
        435=>"Black/Cream", 436=>"Coal", 437=>"Electric/Tarseal", 438=>"Jetsam", 439=>"Mauve", 440=>"Mauve/Cream",
        441=>"Navy/Cream", 442=>"Neo", 443=>"Vanilla", 444=>"Violet", 445=>"Bone", 446=>"Graphite", 447=>"Rust", 448=>"Spec",
        449=>"Syrah", 450=>"10K/Diamond", 451=>"10K/Emerald", 452=>"10K/Ruby", 453=>"10K/Silver/Dia", 454=>"10K/Silver/Ruby",
        455=>"18K", 456=>"Silver/Emerald", 457=>"Stars", 458=>"Stripe/Dark Grey", 459=>"Stripe/Khaki", 460=>"Black Spots",
        461=>"Blue Spot", 462=>"Pattern Black", 463=>"Frostbite", 464=>"Uzi", 465=>"Black/Natural", 466=>"Natural/Black",
        467=>"Bonfire", 468=>"Gothic", 469=>"Lotus", 470=>"Pale Blue", 471=>"DarkDust", 472=>"Rose", 473=>"Black Scrub",
        474=>"Black/Navy", 475=>"Blk/Blk/Floral", 476=>"Cream/Black", 477=>"Graphic/Yellow", 478=>"Gingham/Black", 479=>"Ballet",
        480=>"Basic Black", 481=>"Black Emblem", 482=>"Black Eyelet", 483=>"Black Veil", 484=>"Black/Nickel", 485=>"Blacklawn",
        486=>"Blacksand", 487=>"Crystal", 488=>"Decoritif", 489=>"Emblem", 490=>"Fog", 491=>"Ink Tattoo", 492=>"Ivory",
        493=>"Jet", 494=>"Khol Tattoo", 495=>"Marshmellow", 496=>"Mesh", 497=>"Mist", 498=>"Navy Emblem", 499=>"Porcelain",
        500=>"Saphire", 501=>"Silver Eyelet", 502=>"Taupe", 503=>"Thin Stripe", 504=>"Triple Stripe", 505=>"White Emblem",
        506=>"White Eyelet", 507=>"White Veil", 508=>"Whitelawn", 509=>"Anthracite", 510=>"Black HAHA", 511=>"Black Putty",
        512=>"Dove", 513=>"HAHA", 514=>"HAHA X", 515=>"Nude", 516=>"Putty", 517=>"Putty Black", 518=>"Red Slate", 519=>"Slate",
        520=>"X", 521=>"Green/White", 522=>"Light Orange", 523=>"Orange/Pink", 524=>"Pink/Yellow", 525=>"Yellow/Orange",
        526=>"Dark Dusk", 527=>"Dust", 528=>"Dark Animal", 529=>"Light Animal", 530=>"Black/Stripe", 531=>"Ecru/Black",
        532=>"Grey/Green", 533=>"Red Stripe", 534=>"Green Stripe", 535=>"14ct Gold", 536=>"Concrete", 537=>"Black/Blush",
        538=>"Black Stripe", 539=>"Black/Black Print", 540=>"Black/Gothic", 541=>"Black/Lurex", 542=>"Black/Milk",
        543=>"Black/Pink Print", 544=>"Black/Wallpaper", 545=>"Red Check", 546=>"Wallpaper", 547=>"Black/Putty", 548=>"Chambray",
        549=>"Ashes", 550=>"Basic Navy", 551=>"Black V", 552=>"Blackfleece", 553=>"Blackwood", 554=>"Brushblack", 555=>"Cloud",
        556=>"Coaldust", 557=>"Dark", 558=>"Dark Mix", 559=>"Drill", 560=>"Faux", 561=>"Fine Black", 562=>"Garnet", 563=>"Khol",
        564=>"Labyrinth", 565=>"Navy Fleece", 566=>"Pale Mix", 567=>"Polish", 568=>"Ruby", 569=>"Spotlight", 570=>"Thunderbird",
        571=>"Yellow Fleece", 572=>"Blue Stripes", 573=>"Fluro Yellow", 574=>"Navy Dots", 575=>"Orange/Print", 576=>"Black/Brown",
        577=>"Navy/Stripe", 578=>"Pattern/Black", 579=>"Peach", 580=>"Light Blue", 581=>"Turquoise", 582=>"Chocolate",
        583=>"Off White", 584=>"Rainy Morning", 585=>"Black/Almond", 586=>"Palm", 587=>"Black Angel", 588=>"Black Mix",
        589=>"Chalk", 590=>"Clay Check", 591=>"Covered", 592=>"Dark Ink", 593=>"Flower", 594=>"Ink Angel", 595=>"Ink Mix",
        596=>"Olive Angel", 597=>"Olive Mix", 598=>"Sand", 599=>"Skin", 600=>"Tapestry", 601=>"Lurex", 602=>"Alligator",
        603=>"Basic Grey", 604=>"Black Foil", 605=>"Blackadder", 606=>"Blackbird", 607=>"Charred", 608=>"Eclipse", 609=>"Emerald",
        610=>"Greenacres", 611=>"Iceberg", 612=>"Lacquer", 613=>"Natural", 614=>"Negative", 615=>"Phantom", 616=>"Positive",
        617=>"Smudge", 618=>"Volcanic", 619=>"White Dove", 620=>"Zinc");

    // ToDo: Move mapping to config
    /** @var array self::$sizeById */
    protected static $sizeById = array(4=>'36', 5=>'36.5', 6=>'37', 7=>'37.5', 8=>'38', 9=>'38.5', 10=>'39', 11=>'39.5',
        12=>'40', 13=>'40.5', 14=>'41', 15=>'41.5', 16=>'42', 17=>'42.5', 18=>'43', 19=>'44', 20=>'52', 21=>'55', 22=>'57',
        23=>'67', 24=>'8mm', 25=>'K', 26=>'K.5', 27=>'Q', 28=>'1', 29=>'2', 30=>'3', 31=>'4', 32=>'28', 33=>'46', 34=>'48',
        35=>'50', 36=>'65', 37=>'49', 38=>'61', 39=>'63', 40=>'Zero', 41=>'11', 42=>'13', 43=>'20', 44=>'21', 45=>'22',
        46=>'23', 47=>'24', 48=>'25', 49=>'26', 50=>'27', 51=>'29', 52=>'30', 53=>'31', 54=>'32', 55=>'33', 56=>'34',
        57=>'35', 58=>'45', 59=>'5', 60=>'6', 61=>'7', 62=>'8.5', 63=>'9', 64=>'9.5', 65=>'T1/2', 66=>'L1/2', 67=>'O',
        68=>'22.5', 69=>'23.5', 70=>'24.5', 71=>'25.5', 72=>'4.5', 73=>'5.5', 74=>'6.5', 75=>'35.5', 76=>'7.5', 77=>'O/S',
        78=>'XS', 79=>'8', 80=>'10', 81=>'S', 82=>'M', 83=>'12', 84=>'14', 85=>'L', 86=>'XL', 87=>'16', 88=>'XXL', 89=>'P',
        90=>'N', 91=>'15', 92=>'N/A');


    /**
     * Initialize the gateway and perform any setup actions required.
     * @param string $entityType
     * @return bool $success
     * @throws GatewayException
     */
    protected function _init($entityType)
    {
        $success = parent::_init($entityType);

        if ($entityType != 'product' && $entityType != 'stockitem') {
            throw new GatewayException('Invalid entity type for this gateway');
            $success = FALSE;
        }else{
            try {
                $attributeSets = $this->restV1->get('eav/attribute-sets/list', array(
                    'filter'=>array(array('field'=>'attribute_set_id', 'value'=>0, 'condition_type'=>'gt'))
                ));
            }catch (\Exception $exception) {
                throw new GatewayException($exception->getMessage(), $exception->getCode(), $exception);
                $success = FALSE;
            }

            $this->attributeSets = array();
            foreach ($attributeSets as $attributeSet) {
                $this->attributeSets[$attributeSet['attribute_set_id']] = $attributeSet;
            }

            $this->getServiceLocator()->get('logService')->log(LogService::LEVEL_DEBUG, $this->getLogCode().'_init',
                'Initialised Magento2 product gateway.',
                array('db api'=>(bool) $this->db, 'rest api'=>(bool) $this->restV1,
                    'retrieved attributes'=>$attributeSets, 'stored attributes'=>$this->attributeSets)
            );
        }

        return $success;
    }

    /**
     * @return Magento2Service $this->magento2Service
     */
    protected function getMagento2Service()
    {
        if (is_null($this->magento2Service)) {
            $this->magento2Service = $this->getServiceLocator()->get('magento2Service');
        }

        return $this->magento2Service;
    }

    /**
     * @param $colourId
     * @return string|NULL $colourString
     */
    public static function getColour($colourId)
    {
        return self::getMappedString('colour', (int) $colourId);
    }

    /**
     * @param int $colourString
     * @return int|NULL $colourId
     */
    public static function getColourId($colourString)
    {
        return self::getMappedId('colour', $colourString);
    }

    /**
     * @param $sizeId
     * @return string|NULL $sizeString
     */
    public static function getSize($sizeId)
    {
        return self::getMappedString('size', (int) $sizeId);
    }

    /**
     * @param int $sizeString
     * @return int|NULL $sizeId
     */
    public static function getSizeId($sizeString)
    {
        return self::getMappedId('size', $sizeString);
    }

    /**
     * Retrieve and action all updated records(either from polling, pushed data, or other sources).
     * @return int $numberOfRetrievedEntities
     * @throws GatewayException
     * @throws NodeException
     */
    public function retrieveEntities()
    {
return 0;
        $this->getServiceLocator()->get('logService')
            ->log(LogService::LEVEL_INFO,
                $this->getLogCode().'_re_time',
                'Retrieving products updated since '.$this->lastRetrieveDate,
               array('type'=>'product', 'timestamp'=>$this->lastRetrieveDate)
            );

        $additional = $this->_node->getConfig('product_attributes');
        if (is_string($additional)) {
            $additional = explode(',', $additional);
        }
        if (!$additional || !is_array($additional)) {
            $additional = array();
        }

        if ($this->db) {
            $api = 'db';
            try {
                $updatedProducts = $results = $this->db->getChangedEntityIds('catalog_product', $this->lastRetrieveDate);
            }catch (\Exception $exception) {
                throw new GatewayException($exception->getMessage(), $exception->getCode(), $exception);
            }

            if (count($updatedProducts)) {
                $attributes = array(
                    'sku',
                    'name',
                    'attribute_set_id',
                    'type_id',
                    'description',
                    'short_description',
                    'status',
                    'visibility',
                    'price',
                    'tax_class_id',
                    'special_price',
                    'special_from_date',
                    'special_to_date'
                );

                foreach ($additional as $key=>$attributeCode) {
                    if (!strlen(trim($attributeCode))) {
                        unset($additional[$key]);
                    }elseif (!$this->entityConfigService->checkAttribute('product', $attributeCode)) {
                        $this->entityConfigService->createAttribute(
                            $attributeCode,
                            $attributeCode,
                            FALSE,
                            'varchar',
                            'product',
                            'Magento2 Additional Attribute'
                        );
                        try{
                            $this->_nodeService->subscribeAttribute(
                                $this->_node->getNodeId(),
                                $attributeCode,
                                'product',
                                TRUE
                            );
                        }catch(\Exception $exception) {
                            throw new GatewayException($exception->getMessage(), $exception->getCode(), $exception);
                        }
                    }
                }
                $attributes = array_merge($attributes, $additional);

                foreach ($updatedProducts as $localId) {
                    $sku = NULL;
                    $combinedData = array();
                    $storeIds = array_keys($this->_node->getStoreViews());
// TECHNICAL DEBT // ToDo: Hardcoded to default store
$storeIds = array(current($storeIds));

                    foreach ($storeIds as $storeId) {
                        if ($storeId == 0) {
                            $storeId = FALSE;
                        }

                        $brands = FALSE;
                        if (in_array('brand', $attributes)) {
                            try{
                                $brands = $this->db->loadEntitiesEav('brand', NULL, $storeId, array('name'));
                                if (!is_array($brands) || count($brands) == 0) {
                                    $this->getServiceLocator()->get('logService')->log(LogService::LEVEL_WARN,
                                        $this->getLogCode().'_db_nobrnds',
                                        'Something is wrong with the brands retrieval.',
                                        array('brands'=>$brands)
                                    );
                                    $brands = FALSE;
                                }
                            }catch( \Exception $exception ){
                                $brands = FALSE;
                            }
                        }

                        try{
                            $productsData = $this->db->loadEntitiesEav(
                                'catalog_product', array($localId), $storeId, $attributes);
                            $this->getServiceLocator()->get('logService')->log(LogService::LEVEL_DEBUGEXTRA,
                                $this->getLogCode().'_db_data', 'Loaded product data from Magento2 via DB api.',
                                array('local id'=>$localId, 'store id'=>$storeId, 'data'=>$productsData)
                            );
                        }catch(\Exception $exception) {
                            throw new GatewayException($exception->getMessage(), $exception->getCode(), $exception);
                        }

                        foreach ($productsData as $productId=>$rawData) {
                            // TECHNICAL DEBT // ToDo: Combine this two methods into one
                            $productData = $this->convertFromMagento($rawData, $additional);
                            $productData = $this->getServiceLocator()->get('magento2Service')
                                ->mapProductData($productData, $storeId);

                            if (is_array($brands) && isset($rawData['brand']) && is_numeric($rawData['brand'])) {
                                if (isset($brands[intval($rawData['brand'])])) {
                                    $productData['brand'] = $brands[intval($rawData['brand'])]['name'];
                                }else{
                                    $productData['brand'] = NULL;
                                    $this->getServiceLocator()->get('logService')->log(LogService::LEVEL_WARN,
                                        $this->getLogCode().'_db_nomabra',
                                        'Could not find matching brand for product '.$sku.'.',
                                        array('brand (key)'=>$rawData['brand'], 'brands'=>$brands)
                                    );
                                }
                            }

                            if (isset($rawData['attribute_set_id'])
                                    && isset($this->attributeSets[intval($rawData['attribute_set_id'])])) {
                                $productData['product_class'] = $this->attributeSets[intval(
                                    $rawData['attribute_set_id']
                                )]['attribute_set_name'];
                            }else{
                                $this->getServiceLocator()->get('logService')->log(LogService::LEVEL_WARN,
                                    $this->getLogCode().'_db_noset',
                                    'Issue with attribute set id on product '.$sku.'. Check $rawData[attribute_set_id].',
                                    array('raw data'=>$rawData)
                                );
                            }
                        }

                        if (count($combinedData) == 0) {
                            $sku = $rawData['sku'];
                            $combinedData = $productData;
                        }else {
                            $combinedData = array_replace_recursive($combinedData, $productData, $combinedData);
                        }
                    }

                    $this->getServiceLocator()->get('logService')
                        ->log(LogService::LEVEL_DEBUGEXTRA, $this->getLogCode().'_db_comb',
                            'Combined data for Magento2 product id '.$localId.'.',
                            array('combined data'=>$combinedData)
                        );

                    $parentId = NULL; // TECHNICAL DEBT // ToDo: Calculate

                    try{
                        $this->processUpdate($productId, $sku, $storeId, $parentId, $combinedData);
                    }catch( \Exception $exception ){
                        // store as sync issue
                        throw new GatewayException($exception->getMessage(), $exception->getCode(), $exception);
                    }
                }
            }
        }elseif ($this->restV1) {
            $api = 'restV1';
            // TECHNICAL DEBT // ToDo: Multistore capability!
            $storeId = NULL;
            try {
                $results = $this->restV1->get('product', array(
                    'filter'=>array(array(
                        'field'=>'updated_at',
                        'value'=>$this->lastRetrieveDate,
                        'condition_type'=>'gt'
                    ))
                ));
            }catch (\Exception $exception) {
                throw new GatewayException($exception->getMessage(), $exception->getCode(), $exception);
            }

            foreach ($results as $productData) {
                $productId = $productData['product_id'];
                $sku = $productData['sku'];

                // TECHNICAL DEBT // ToDo
                $productData = array_merge(
                    $productData,
                    $this->loadFullProduct($sku, $storeId)
                );

                $this->getServiceLocator()->get('logService')->log(LogService::LEVEL_DEBUGEXTRA,
                    $this->getLogCode().'_rest_data', 'Loaded product data from Magento2 via SOAP api.',
                    array('sku'=>$productData['sku'], 'data'=>$productData)
                );

                if (isset($this->attributeSets[intval($productData['set']) ])) {
                    $productData['product_class'] = $this->attributeSets[intval($productData['set']) ]['name'];
                    unset($productData['set']);
                }else{
                    $this->getServiceLocator()->get('logService')
                        ->log(LogService::LEVEL_WARN,
                            $this->getLogCode().'_rest_uset',
                            'Unknown attribute set ID '.$productData['set'],
                           array('set'=>$productData['set'], 'sku'=>$productData['sku'])
                        );
                }

                if (isset($productData[''])) {
                    unset($productData['']);
                }

                unset($productData['category_ids']); // TECHNICAL DEBT // ToDo parse into categories
                unset($productData['website_ids']); // Not used

                $productId = $productData['product_id'];
                $parentId = NULL; // TECHNICAL DEBT // ToDo: Calculate
                $sku = $productData['sku'];
                unset($productData['product_id']);
                unset($productData['sku']);

                try {
                    $this->processUpdate($productId, $sku, $storeId, $parentId, $productData);
                }catch (\Exception $exception) {
                    // store as sync issue
                    throw new GatewayException($exception->getMessage(), $exception->getCode(), $exception);
                }
            }
        }else{
            throw new NodeException('No valid API available for sync');
            $api = '-';
        }

        $this->_nodeService
            ->setTimestamp($this->_nodeEntity->getNodeId(), 'product', 'retrieve', $this->getNewRetrieveTimestamp());

        $seconds = ceil($this->getAdjustedTimestamp() - $this->getNewRetrieveTimestamp());
        $message = 'Retrieved '.count($results).' products in '.$seconds.'s up to '
            .strftime('%H:%M:%S, %d/%m', $this->retrieveTimestamp).' via '.$api.' api.';
        $logData = array('type'=>'product', 'amount'=>count($results), 'period [s]'=>$seconds);
        if (count($results) > 0) {
            $logData['per entity [s]'] = round($seconds / count($results), 3);
        }
        $this->getServiceLocator()->get('logService')->log(LogService::LEVEL_INFO, $this->getLogCode().'_re_no', $message, $logData);
    }

    /**
     * @param int $productId
     * @param string $sku
     * @param int $storeId
     * @param int $parentId
     * @param array $data
     * @return \Entity\Entity|NULL
     */
    protected function processUpdate($productId, $sku, $storeId, $parentId, array $data)
    {
        /** @var boolean $needsUpdate Whether we need to perform an entity update here */
        $needsUpdate = TRUE;

        $existingEntity = $this->_entityService->loadEntityLocal($this->_node->getNodeId(), 'product', 0, $productId);
        if (!$existingEntity) {
            $existingEntity = $this->_entityService->loadEntity($this->_node->getNodeId(), 'product', 0, $sku);
            $noneOrWrongLocalId = $this->_entityService->getLocalId($this->_node->getNodeId(), $existingEntity);

            if (!$existingEntity) {
                $existingEntity = $this->_entityService
                    ->createEntity($this->_node->getNodeId(), 'product', 0, $sku, $data, $parentId);
                $this->_entityService->linkEntity($this->_node->getNodeId(), $existingEntity, $productId);
                $this->getServiceLocator()->get('logService')
                    ->log(LogService::LEVEL_INFO,
                        $this->getLogCode().'_new',
                        'New product '.$sku,
                       array('sku'=>$sku),
                       array('node'=>$this->_node, 'entity'=>$existingEntity)
                    );
                try{
                    $stockEntity = $this->_entityService
                        ->createEntity($this->_node->getNodeId(), 'stockitem', 0, $sku, array(), $existingEntity);
                    $this->_entityService->linkEntity($this->_node->getNodeId(), $stockEntity, $productId);
                }catch (\Exception $exception) {
                    $this->getServiceLocator() ->get('logService')
                        ->log(LogService::LEVEL_WARN,
                            $this->getLogCode().'_si_ex',
                            'Already existing stockitem for new product '.$sku,
                           array('sku'=>$sku),
                           array('node'=>$this->_node, 'entity'=>$existingEntity)
                        );
                }
                $needsUpdate = FALSE;
            }elseif ($noneOrWrongLocalId != NULL) {
                $this->_entityService->unlinkEntity($this->_node->getNodeId(), $existingEntity);
                $this->_entityService->linkEntity($this->_node->getNodeId(), $existingEntity, $productId);

                $stockEntity = $this->_entityService->loadEntity($this->_node->getNodeId(), 'stockitem', 0, $sku);
                if ($this->_entityService->getLocalId($this->_node->getNodeId(), $stockEntity) != NULL) {
                    $this->_entityService->unlinkEntity($this->_node->getNodeId(), $stockEntity);
                }
                $this->_entityService->linkEntity($this->_node->getNodeId(), $stockEntity, $productId);

                $this->getServiceLocator() ->get('logService')
                    ->log(LogService::LEVEL_ERROR,
                        $this->getLogCode().'_relink',
                        'Incorrectly linked product '.$sku.' ('.$noneOrWrongLocalId.'). Re-linked now.',
                       array('code'=>$sku, 'wrong local id'=>$noneOrWrongLocalId),
                       array('node'=>$this->_node, 'entity'=>$existingEntity)
                    );
            }else{
                $this->getServiceLocator() ->get('logService')
                    ->log(LogService::LEVEL_INFO,
                        $this->getLogCode().'_link',
                        'Unlinked product '.$sku,
                       array('sku'=>$sku),
                       array('node'=>$this->_node, 'entity'=>$existingEntity)
                    );
                $this->_entityService->linkEntity($this->_node->getNodeId(), $existingEntity, $productId);
            }
        }else{
            $this->getServiceLocator()->get('logService')
                ->log(LogService::LEVEL_INFO,
                    $this->getLogCode().'_upd',
                    'Updated product '.$sku,
                   array('sku'=>$sku),
                   array('node'=>$this->_node, 'entity'=>$existingEntity, 'data'=>$data)
                );
        }

        if ($needsUpdate) {
            $this->_entityService->updateEntity($this->_node->getNodeId(), $existingEntity, $data, FALSE);
        }

        return $existingEntity;
    }

    /**
     * Load detailed product data from Magento2
     * @param $productId
     * @param $storeId
     * @param \Entity\Service\EntityConfigService $this->entityConfigService
     * @return array
     * @throws \Magelink\Exception\MagelinkException
     */
    public function loadFullProduct($sku, $storeId) {

        $additional = $this->_node->getConfig('product_attributes');
        if (is_string($additional)) {
            $additional = explode(',', $additional);
        }
        if (!$additional || !is_array($additional)) {
            $additional = array();
        }

        // 'custom_attributes'
        $data = array(
            $storeId,
            array('additional_attributes'=>$additional),
            'id',
        );

        $productInfo = $this->restV1->get('products/'.$sku, $data);

        if (!$productInfo && !$productInfo['sku']) {
            // store as sync issue
            throw new GatewayException('Invalid product info response');
            $data = NULL;
        }else{
            $data = $this->convertFromMagento2($productInfo, $additional);

            foreach ($additional as $attributeCode) {
                $attributeCode = strtolower(trim($attributeCode));

                if (strlen($attributeCode)) {
                    if (!array_key_exists($attributeCode, $data)) {
                        $data[$attributeCode] = NULL;
                    }

                    if (!$this->entityConfigService->checkAttribute('product', $attributeCode)) {
                        $this->entityConfigService->createAttribute(
                            $attributeCode,
                            $attributeCode,
                            0,
                            'varchar',
                            'product',
                            'Custom Magento2 attribute'
                        );

                        try {
                            $this->getServiceLocator()->get('nodeService')->subscribeAttribute(
                                $this->_node->getNodeId(),
                                $attributeCode,
                                'product'
                            );
                        }catch (\Exception $exception) {
                            // Store as sync issue
                            throw new GatewayException($exception->getMessage(), $exception->getCode(), $exception);
                            $data = NULL;
                        }
                    }
                }
            }
        }

        return $data;
    }

    /**
     * Converts Magento2-named attributes into our internal Magelink attributes / formats.
     * @param array $rawData Input array of Magento2 attribute codes
     * @param array $additional Additional product attributes to load in
     * @return array
     */
    protected function convertFromMagento($rawData, $additional)
    {
        $data = array();

        if (isset($rawData['additional_attributes'])) {
            foreach ($rawData['additional_attributes'] as $pair) {
                $attributeCode = trim(strtolower($pair['key']));
                if (!in_array($attributeCode, $additional)) {
                    throw new GatewayException('Invalid attribute returned by Magento2: '.$attributeCode);
                }
                if (isset($pair['value'])) {
                    $rawData[$attributeCode] = $pair['value'];
                }else {
                    $rawData[$attributeCode] = null;
                }
            }
        }else{
            foreach ($additional as $code) {
                if (isset($rawData[$code])) {
                    $data[$code] = $rawData[$code];
                }
            }
        }

        if (isset($rawData['type_id'])) {
            $data['type'] = $rawData['type_id'];
        }elseif (isset($rawData['type'])) {
            $data['type'] = $rawData['type'];
        }else{
            $data['type'] = NULL;
        }
        if (isset($rawData['name'])) {
            $data['name'] = $rawData['name'];
        }else{
            $data['name'] = NULL;
        }
        if (isset($rawData['description'])) {
            $data['description'] = $rawData['description'];
        }else{
            $data['description'] = NULL;
        }
        if (isset($rawData['short_description'])) {
            $data['short_description'] = $rawData['short_description'];
        }else{
            $data['short_description'] = NULL;
        }
        if (isset($rawData['status'])) {
            $data['enabled'] = ($rawData['status'] == 1) ? 1 : 0;
        }else{
            $data['enabled'] = 0;
        }
        if (isset($rawData['visibility'])) {
            $data['visible'] = ($rawData['visibility'] == 4) ? 1 : 0;
        }else{
            $data['visible'] = 0;
        }
        if (isset($rawData['price'])) {
            $data['price'] = $rawData['price'];
        }else{
            $data['price'] = NULL;
        }
        if (isset($rawData['tax_class_id'])) {
            $data['taxable'] = ($rawData['tax_class_id'] == 2) ? 1 : 0;
        }else{
            $data['taxable'] = 0;
        }
        if (isset($rawData['special_price'])) {
            $data['special_price'] = $rawData['special_price'];

            if (isset($rawData['special_from_date'])) {
                $data['special_from_date'] = $rawData['special_from_date'];
            }else{
                $data['special_from_date'] = NULL;
            }
            if (isset($rawData['special_to_date'])) {
                $data['special_to_date'] = $rawData['special_to_date'];
            }else{
                $data['special_to_date'] = NULL;
            }
        }else{
            $data['special_price'] = NULL;
            $data['special_from_date'] = NULL;
            $data['special_to_date'] = NULL;
        }

        if (isset($rawData['color'])) {
            $data['color'] = self::getColour($rawData['color']);
        }
        if (isset($rawData['size'])) {
            $data['size'] = self::getSize($rawData['size']);
        }

        return $data;
    }

    /**
     * @param Product $product
     * @param int $type
     * @return array $productData
     */
    protected function getProductWriteData(Product $product, $type)
    {
        // TECHNICAL DEBT // ToDo : change this into a mapping

        $data = array();

        foreach ($product->getFullArrayCopy() as $code=>$value) {
            $mappedCode = $this->getMagento2Service()->getMappedCode('product', $code);
            switch ($mappedCode) {
                case 'price':
                case 'special_price':
                case 'special_from_date':
                case 'special_to_date':
                    $value = ($value ? $value : NULL);
                case 'name':
                case 'description':
                case 'short_description':
                case 'weight':
                case 'barcode':
                case 'bin_location':
                case 'msrp':
                case 'cost':
                    // Same name in both systems
                    $data[$code] = $value;
                    break;
                case 'enabled':
                    if ($value < 0) {
                        // Ignore status
                        unset($data['status']);
                    }else{
                        $data['status'] = ($value == 1 ? 1 : 2);
                    }
                    break;
                case 'taxable':
                    $data['tax_class_id'] = ($value == 1 ? 2 : 1);
                    break;
                case 'visible':
                    $data['visibility'] = ($value == 1 ? 4 : 1);
                    break;
                case 'color':
                    $data['color'] = self::getColourId($value);
                    break;
                case 'size':
                    $data['size'] = self::getSizeId($value);
                    break;
                // TECHNICAL DEBT // ToDo (maybe) : Add logic for this custom attributes
                case 'brand':
                case 'product_class':
                    // Ignore attributes
                    break;
                case 'type':
                    if ($type != Update::TYPE_CREATE) {
                        $data['type_id'] = $value;
                    }
                    break;
                default:
                    $this->getServiceLocator()->get('logService')
                        ->log(LogService::LEVEL_WARN,
                            $this->getLogCode().'_wr_invdata',
                            'Unsupported attribute for update of '.$product->getUniqueId().': '.$code,
                            array('type'=>'product', 'attribute'=>$code),
                            array('entity'=>$product)
                        );
            }
        }

        return $data;
    }

    /**
     * @param Entity $stockitem
     * @return array $stockitemData
     */
    protected function getStockitemWriteData(Entity $stockitem)
    {
        // TECHNICAL DEBT // ToDo : Move that to a stock gateway
        // TECHNICAL DEBT // ToDo : change this into a mapping

        $data = array();

        foreach ($stockitem->getFullArrayCopy() as $code=>$value) {
            switch ($code) {
                case 'available':
                    $data['qty'] = $value;
                    $data['is_in_stock'] = ($value > 0 ? 1 : 0);
                    break;
                case 'qty_soh':
                case 'total':
                    // Ignore attributes
                    break;
                default:
                    $this->getServiceLocator()->get('logService')
                        ->log(
                            LogService::LEVEL_WARN,
                            $this->getLogCode().'_wr_invdata',
                            'Unsupported attribute for update of '.$stockitem->getUniqueId().': '.$code,
                            array('type'=>'stockitem', 'attribute'=>$code),
                            array('entity'=>$stockitem)
                        );
            }
        }

        return $data;
    }


    /**
     * Restructure data for rest call and return this array.
     * @param \Entity\Entity $entity
     * @param array $data
     * @param array $customAttributeCodes
     * @return array $restData
     * @throws \Magelink\Exception\MagelinkException
     */
    protected function getUpdateDataForRestCall(\Entity\Entity $entity, array $data, array $customAttributeCodes)
    {
        $sku = $entity->getUniqueId();
        if (!isset($sku)) {
            throw new GatewayException('SKU is essential for a synchronisation but missing.');
            $restData = array();

        }else{
            $restData = $data;
            $customAttributes = array();
            $customAttributeCodes = array_merge(
                $customAttributeCodes,
                array('special_price', 'special_from_date', 'special_to_date')
            );
            $rootAttributes = array('id', 'sku', 'name', 'price', 'weight',
                'attribute_set_id', 'status', 'type_id', 'visibility', 'created_at', 'updated_at');

            foreach ($data as $code=>$value) {
                $isCustomAttribute = in_array($code, $customAttributeCodes) || !in_array($code, $rootAttributes);
                if (is_null($value)) {
                    unset($restData[$code]);
                }elseif ($isCustomAttribute && is_array($data[$code])) {
                    // ToDo(maybe) : Implement
                    $message = 'This gateway doesn\'t support multi data custom attributes yet: '.$code.'.';
                    $this->getServiceLocator()->get('logService')
                        ->log(LogService::LEVEL_ERROR, $this->getLogCode().'_crat_err', $message,
                            array('type'=>$entity->getTypeStr(), 'code'=>$code, 'value'=>$value),
                            array('entity'=>$entity, 'custom attributes'=>$customAttributeCodes)
                        );
                }elseif ($isCustomAttribute) {
                    $customAttributes[$code] = array('attribute_code'=>$code, 'value'=>$value);
                    unset($restData[$code]);
                }
            }

            $restData['sku'] = $sku;

            if (!isset($customAttributes['special_price'])) {
                unset($customAttributes['special_from_date'], $customAttributes['special_to_date']);
            }

            if (count($customAttributes) > 0) {
                $restData['custom_attributes'] = array_values($customAttributes);
            }

            unset($restData['website_ids']);
        }

        return $restData;
    }

    /**
     * Write out all the updates to the given entity.
     * @param \Entity\Entity $entity
     * @param string[] $attributes
     * @param int $type
     */
    public function writeUpdates(\Entity\Entity $entity, $attributes, $type = Update::TYPE_UPDATE)
    {
        $nodeId = $this->_node->getNodeId();
        $sku = $entity->getUniqueId();

        if ($entity->getTypeStr() == 'product') {
            $product = $entity;
            $stockitem = $this->_entityService->loadEntity($nodeId, 'stockitem', 0, $sku);
        }elseif ($entity->getTypeStr() == 'stockitem') {
            $stockitem = $entity;
            $product = $stockitem->getParent();
        }else{
            throw new GatewayException('Wrong entity type: '.$entity->getTypeStr().'.');
            $entity = NULL;
        }

        $customAttributes = $this->_node->getConfig('product_attributes');
        if (is_string($customAttributes)) {
            $customAttributes = explode(',', $customAttributes);
        }
        if (!$customAttributes || !is_array($customAttributes)) {
            $customAttributes = array();
        }

        $this->getServiceLocator()->get('logService')
            ->log(LogService::LEVEL_DEBUGEXTRA,
                $this->getLogCode().'_wrupd',
                'Attributes for update of product '.$sku.': '.var_export($attributes, TRUE),
               array('attributes'=>$attributes, 'custom'=>$customAttributes),
               array('entity'=>$product)
            );

        $originalData = $product->getFullArrayCopy();
        $attributeCodes = array_unique(array_merge(
            //array('special_price', 'special_from_date', 'special_to_date'), // force update of these attributes
            //$customAttributes,
            $attributes
        ));

        $data = array();
        if (count($originalData) == 0) {
            $this->getServiceLocator()->get('logService')
                ->log(LogService::LEVEL_INFO,
                    $this->getLogCode().'_wrupd_non',
                    'No update required for '.$sku.' but requested was '.implode(', ', $attributes),
                    array('attributes'=>$attributes),
                    array('entity'=>$product)
                );
        }else{
            $localId = $this->_entityService->getLocalId($this->_node->getNodeId(), $product);
            $data = $this->getProductWriteData($product, $type);
            $stockitemData = $this->getStockitemWriteData($stockitem);

            $storeDataByStoreId = $this->_node->getStoreViews();
// TECHNICAL DEBT // ToDo: Hardcoded to default store
if (isset($storeDataByStoreId[0])) { $storeDataByStoreId = array(0=>$storeDataByStoreId[0]); }else{ $storeDataByStoreId = array(0=>current($storeDataByStoreId)); }

            if (count($storeDataByStoreId) > 0 && $type != Update::TYPE_DELETE) {
                $websiteIds = array();
                $dataPerStore = array();

                foreach ($storeDataByStoreId as $storeId=>$storeData) {
                    if ($storeId > 0) {
                        foreach (array('price', 'special_price', 'msrp', 'cost') as $code) {
                            unset($data[$code]);
                        }
                    }

                    $dataToMap = $this->getMagento2Service()->mapProductData($data, $storeId, FALSE, TRUE);

                    if ($this->getMagento2Service()->isStoreUsingDefaults($storeId)) {
                        $dataToCheck = $data;
                    }else{
                        $dataToCheck = $dataToMap;
                    }

                    $isEnabled = isset($dataToCheck['price']);
                    if ($isEnabled) {
                        $websiteIds[] = $storeData['website_id'];
                        $logCode = $this->getLogCode().'_wrupd_wen';
                        $logMessage = 'enabled';
                    }else{
                        $logCode = $this->getLogCode().'_wrupd_wdis';
                        $logMessage = 'disabled';
                    }

                    $logMessage = 'Product '.$sku.' will be '.$logMessage.' on website '.$storeData['website_id'].'.';
                    $logData = array('store id'=>$storeId, 'dataToMap'=>$dataToMap, 'dataToCheck'=>$dataToCheck);

                    $this->getServiceLocator()->get('logService')
                        ->log(LogService::LEVEL_DEBUGINTERNAL, $logCode, $logMessage, $logData);
                    $dataPerStore[$storeId] = $dataToMap;
                }
                unset($dataToMap, $dataToCheck);

                $storeIds = array_unique(array_merge(array(0), array_keys($storeDataByStoreId)));

                $this->getServiceLocator()->get('logService')->log(LogService::LEVEL_DEBUGINTERNAL,
                    $this->getLogCode().'_wrupd_stor',
                    'StoreIds '.json_encode($storeIds).' (type: '.$type.'), websiteIds '.json_encode($websiteIds).'.',
                    array('store data'=>$storeDataByStoreId)
                );

                foreach ($storeIds as $storeId) {
                    $productData = $dataPerStore[$storeId];

                    if ($storeId != 0 || $this->getMagento2Service()->isStoreUsingDefaults($storeId)) {
                        unset($productData['special_price']);
                        unset($productData['special_from_date']);
                        unset($productData['special_to_date']);
                    }

                    $restData = $this->getUpdateDataForRestCall($product, $productData, $customAttributes);
                    $restData['extension_attributes']['stock_item'] = $stockitemData;

                    $logData = array(
                        'type'=>$entity->getData('type'),
                        'store id'=>$storeId,
                        'product data'=>$productData,
                        'restData'=>$restData
                    );

                    if ($type == Update::TYPE_UPDATE) {
                        foreach ($productData as $attributeCode=>$attributeValue) {
                            if (!in_array($attributeCode, $attributeCodes)) {
                                unset($productData[$attributeCode]);
                            }
                        }
                        $updateRestData = $this->getUpdateDataForRestCall($product, $productData, $customAttributes);
                        $updateRestData['extension_attributes']['stock_item'] = $stockitemData;

                        if (count($updateRestData) == 0) {
                            // ToDo: Check if products exists remotely
                                // if not unset($localId) and change type to Update::TYPE_CREATE

                            $logData['updateRestData'] = $updateRestData;
                        }
                    }

                    $restResult = NULL;

                    $updateViaDbApi = ($this->db && $localId && $storeId == 0);
                    if ($updateViaDbApi) {
                        $api = 'db';
                    }else{
                        $api = 'restV1';
                    }

                    if ($type == Update::TYPE_UPDATE || $localId) {
                        if ($updateViaDbApi) {
                            try{
                                $tablePrefix = 'catalog_product';
                                $rowsAffected = $this->db->updateEntityEav(
                                    $tablePrefix,
                                    $localId,
                                    $product->getStoreId(),
                                    $productData
                                );

                                if ($rowsAffected != 1) {
                                    throw new MagelinkException($rowsAffected.' rows affected.');
                                }
                            }catch(\Exception $exception) {
                                $this->_entityService->unlinkEntity($nodeId, $product);
                                $localId = NULL;
                                $updateViaDbApi = FALSE;
                            }
                        }

                        $logMessage = 'Updated product '.$sku.' on store '.$storeId.' ';
                        if ($updateViaDbApi) {
                            $logLevel = LogService::LEVEL_INFO;
                            $logCode = $this->getLogCode().'_wrupddb';
                            $logMessage .= 'successfully via DB api with '.implode(', ', array_keys($productData));
                        }else{
                            try{
                                $putData = array('product'=>$updateRestData);
                                $restResult = array('update'=>
                                    $this->restV1->put('products/'.$sku, $putData));

                                if (is_null($localId) && isset($restResult['update']['id'])) {
                                    $type = Update::TYPE_UPDATE;
                                    $localId = $restResult['update']['id'];
                                    $this->_entityService->linkEntity($nodeId, $product, $localId);
                                }
                            }catch(\Exception $exception) {
                                $restResult = FALSE;
                                $type = Update::TYPE_CREATE;

                                $logLevel = ($restResult ? LogService::LEVEL_INFO : LogService::LEVEL_ERROR);
                                $logCode = $this->getLogCode().'_wrupdrest';
                            }

                            if (!$restResult) {
                                $logMessage = $api.' update failed. Removed local id '.$localId
                                    .' from node '.$nodeId.'. '.$logMessage;
                                if (isset($exception)) {
                                    $logData[strtolower($api.' error')] = $exception->getMessage();
                                }

                                $this->_entityService->unlinkEntity($nodeId, $product);
                                $localId = NULL;
                            }

                            $logMessage .= ($restResult ? 'successfully' : 'without success').' via ReST api.'
                                .($type == Update::TYPE_CREATE ? ' Try to create now.' : '');
                            $logData['rest data'] = $restData;
                            $logData['rest result'] = $restResult;
                        }
                        $this->getServiceLocator()->get('logService')->log($logLevel, $logCode, $logMessage, $logData);
                    }

                    if ($type == Update::TYPE_CREATE) {

                        foreach ($this->attributeSets as $setId=>$set) {
                            $setName = $set['attribute_set_name'];
                            $productClass = $product->getData('product_class', 'default');

                            $isNameMatching = strtolower($setName) == strtolower($productClass);
                            $hasProductType = $set['entity_type_id'] == 4;

                            if ($isNameMatching && $hasProductType) {
                                $restData['attribute_set_id'] = $setId;
                                break;
                            }
                        }

                        if (!isset($restData['attribute_set_id'])) {
                            $message = 'Invalid product class '.$product->getData('product_class', 'default');
                            throw new \Magelink\Exception\SyncException($message);
                        }

                        $message = 'Creating product (ReST) : '.$sku.' with '.implode(', ', array_keys($productData));
                        $logData['set'] = $restData['attribute_set_id'];
                        $this->getServiceLocator()->get('logService')
                            ->log(LogService::LEVEL_INFO, $this->getLogCode().'_wr_cr', $message, $logData);

                        try{
                            $postData = array(
                                'product'=>$restData,
                                'saveOptions'=>TRUE
                            );

                            $restResult = $this->restV1->post('products', $postData);
                            $restFault = NULL;
                        }catch(\Exception $exception) {
                            if (is_null($restFault = $exception->getPrevious())) {
                                $restFault = $exception;
                            }
                            $restFaultMessage = $restFault->getMessage();
                            $restResult = FALSE;

                            if ($restFaultMessage == 'The value of attribute "SKU" must be unique') {
                                $this->getServiceLocator()->get('logService')
                                    ->log(LogService::LEVEL_WARN,
                                        $this->getLogCode().'_wr_duperr',
                                        'Creating product '.$sku.' hit SKU duplicate fault',
                                        array(),
                                        array('entity'=>$product, 'rest fault'=>$restFault)
                                    );

                                $check = $this->restV1->get('products/'.$sku, array());
                                if (!$check || !count($check)) {
                                    $message = 'Magento2 complained duplicate SKU but we cannot find a duplicate!';
                                    throw new MagelinkException($message);

                                }else{
                                    $found = FALSE;
                                    foreach ($check as $row) {
                                        if ($row['sku'] == $sku) {
                                            $found = TRUE;

                                            $this->_entityService->linkEntity($nodeId, $product, $row['product_id']);
                                            $this->getServiceLocator()->get('logService')
                                                ->log(LogService::LEVEL_INFO,
                                                    $this->getLogCode().'_wr_dupres',
                                                    'Creating product '.$sku.' resolved SKU duplicate fault',
                                                    array('local_id'=>$row['product_id']),
                                                    array('entity'=>$product)
                                                );
                                        }
                                    }

                                    if (!$found) {
                                        $message = 'Magento2 found duplicate SKU '.$sku
                                            .' but we could not replicate. Database fault?';
                                        throw new MagelinkException($message);
                                    }
                                }
                            }
                        }

                        if ($restResult) {
                            if (isset($restResult['id'])) {
                                $localId = $restResult['id'];
                            }else{
                                $localId = NULL;
                            }

                            $this->_entityService->linkEntity($nodeId, $product, $localId);

                            $logData['rest data'] = $restData;
                            $this->getServiceLocator()->get('logService')->log(LogService::LEVEL_INFO,
                                $this->getLogCode().'_wr_loc_id',
                                'Added product local id '.$localId.' for '.$sku.' ('.$nodeId.')',
                                $logData
                            );
                        }else{
                            $message = 'Error creating product '.$sku.' in Magento2!';
                            throw new MagelinkException($message, 0, $restFault);
                        }
                    }
                }
                unset($dataPerStore);
            }
        }
    }

    /**
     * Write out the given action.
     * @param Action $action
     * @throws MagelinkException
     */
    public function writeAction(Action $action)
    {
        $entity = $action->getEntity();
        switch($action->getType()) {
            case 'delete':
                $this->restV1->delete('products/'.$entity->getUniqueId());
                $success = TRUE;
                break;
            default:
                throw new MagelinkException('Unsupported action type '.$action->getType().' for Magento2 Orders.');
                $success = FALSE;
        }

        return $success;
    }

}
