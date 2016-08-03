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

use Entity\Update;
use Entity\Action;
use Magento2\Service\Magento2Service;
use Log\Service\LogService;
use Magelink\Exception\MagelinkException;
use Magelink\Exception\SyncException;
use Magelink\Exception\NodeException;
use Magelink\Exception\GatewayException;
use Node\Entity;


class ProductGateway extends AbstractGateway
{
    const GATEWAY_ENTITY = 'product';
    const GATEWAY_ENTITY_CODE = 'p';

    /** @var array $this->attributeSets */
    protected $attributeSets;

    // ToDo: Move mapping to config
    /** @var array self::$colourById */
    protected static $colourById = array(93=>'Alabaster', 94=>'AmG/Gran', 95=>'AmGr/Blk', 96=>'AmGrn', 97=>'Am/Car',
        98=>'Am/Des', 99=>'Aniseed', 100=>'Army', 101=>'ArmyGran', 102=>'ArmyGreen', 103=>'Ash', 104=>'Beige', 105=>'Black',
        106=>'BlackCroc', 107=>'BlackMagic', 108=>'BlackMarle', 109=>'BlackPony', 110=>'BlackSheep', 111=>'BlackTwill',
        112=>'Black/Black', 113=>'Black/Blue', 114=>'Black/Brass', 115=>'Black/Burg.Gingham', 116=>'Black/Burgundy',
        117=>'Black/Char.Lurex', 118=>'Black/Charcoal', 119=>'Black/Check', 120=>'Black/Dark', 121=>'Black/Fluoro',
        122=>'Black/Glow', 123=>'Black/Gold', 124=>'Black/Green', 125=>'Black/Grey', 126=>'Black/Ink', 127=>'Black/Light',
        128=>'Black/Multi', 129=>'Black/Org', 130=>'Black/Pearl', 131=>'Black/Pink', 132=>'Black/Pitch', 133=>'Black/PitchTartan',
        134=>'Black/Plum', 135=>'Black/Pop', 136=>'Black/Poppy', 137=>'Black/Purple', 138=>'Black/Red', 139=>'Black/Silver',
        140=>'Black/Tartan', 141=>'Black/Violet', 142=>'Black/White', 143=>'Black/Yellow', 144=>'Blackball', 145=>'Blackboard',
        146=>'Blackcheck', 147=>'Blacken', 148=>'Blackfelt', 149=>'Blackfelt/Leather', 150=>'Blackout', 151=>'Blackpool',
        152=>'Blackstretch', 153=>'Blacksuit', 154=>'Blackwash', 155=>'Blackwax', 156=>'BlackYellow', 157=>'Bleach',
        158=>'Blk/Rattlesnake', 159=>'BloodStone', 160=>'BluChk', 161=>'Blue', 162=>'BlueLogo', 163=>'BlueMix', 164=>'BlueTartan',
        165=>'Bluegum/Licorice', 166=>'Bluescreen', 167=>'Blush', 168=>'Bonded', 169=>'Brass', 170=>'Brass/Black',
        171=>'Brass/Brass', 172=>'Brick', 173=>'Bronze', 174=>'Brown', 175=>'BrownLogo', 176=>'BrownMulti', 177=>'BrownSnake',
        178=>'BrownTartan', 179=>'Brownie', 180=>'Buff', 181=>'Burg.Ging/Espresso', 182=>'Burgundy', 183=>'BurgundyGingham',
        184=>'Burgundy/Black', 185=>'Burgundy/Espresso', 186=>'Burgundy/Gingham', 187=>'Camo', 188=>'Candy', 189=>'Carbon',
        190=>'CargoGreen', 191=>'Caviar', 192=>'CharGran', 193=>'CharTri', 194=>'Char/Utility', 195=>'Charcoal', 196=>'Cherry',
        197=>'Chilli', 198=>'Coffee', 199=>'Coral', 200=>'Cream', 201=>'CrystalBlackC', 202=>'CrystalWhiteC', 203=>'Crystal/Pcorn',
        204=>'Crystal/Walnut', 205=>'DarkBlue', 206=>'DarkBrown', 207=>'DarkDust', 208=>'DarkTweed', 209=>'DarlIndigo',
        210=>'Dash/Black', 211=>'Delft', 212=>'Desert', 213=>'DesertMix', 214=>'DesertPJ', 215=>'DesertUtilityMix', 216=>'Dusk',
        217=>'Ebony', 218=>'Ecru', 219=>'Electric', 220=>'Flesh', 221=>'Flint', 222=>'Floral', 223=>'Forest', 224=>'Fuchsia/Pop',
        225=>'Fudge', 226=>'Glow', 227=>'Gold', 228=>'Green', 229=>'GreenMix', 230=>'Green/Grey', 231=>'Grey', 232=>'GreyMarle',
        233=>'Grey/Blue', 234=>'Grey/Ink', 235=>'Grey/Navy', 236=>'Grey/Pink', 237=>'Grey/Purple', 238=>'Hands/Black',
        239=>'HomemadeBlack', 240=>'Ice', 241=>'Indigo', 242=>'Ink', 243=>'Ink/Black', 244=>'Iron', 245=>'Jade', 246=>'Jetblack',
        247=>'Kelp', 248=>'Khaki', 249=>'Kidblack', 250=>'Lapis', 251=>'Lateshow', 252=>'Lavender', 253=>'LightGrey',
        254=>'LightMix', 255=>'Lime', 256=>'Logo', 257=>'MadWax', 258=>'Magenta', 259=>'Mahogany', 260=>'Maroon',
        261=>'MattBlack', 262=>'MattGrey', 263=>'Melon', 264=>'Metal', 265=>'Midnight', 266=>'Midnight/Black', 267=>'Military',
        268=>'Milk', 269=>'MixedChk', 270=>'Monster', 271=>'Multi', 272=>'Mushroom', 273=>'Mustard', 274=>'N/A', 275=>'Navy',
        276=>'NavyCheck', 277=>'Navy/Black', 278=>'Navy/Ivory', 279=>'Navy/White', 280=>'Nickel', 281=>'Noir', 282=>'Nori',
        283=>'Olive', 284=>'Onyx', 285=>'Orange', 286=>'OrangeLogo', 287=>'OrangePony', 288=>'Oyster', 289=>'P/M/Pew',
        290=>'P/M/Pink', 291=>'Paint/Black', 292=>'Papaya', 293=>'Passport', 294=>'Pearl', 295=>'Peat', 296=>'Peat/Black',
        297=>'Petrol', 298=>'Petrol/Black', 299=>'Petrol/Charcoal', 300=>'Pew/Pk', 301=>'Pewt/Gran', 302=>'Pewt/Pewt',
        303=>'Pewt/Tri', 304=>'Pewter', 305=>'Pewter/Pewter', 306=>'Pewter/Tri', 307=>'Pink', 308=>'PinkMix', 309=>'Pink/Tri',
        310=>'Pirate', 311=>'Pitch', 312=>'PitchTartan', 313=>'PitchTartan/Black', 314=>'Pitch/Black', 315=>'Pitch/Tartan',
        316=>'PJPrint', 317=>'Pk/Flor', 318=>'Pk/Pew', 319=>'Plum', 320=>'Plum/Black', 321=>'Plum/Espresso', 322=>'Plum/Gingham',
        323=>'Pop/White', 324=>'Poppy', 325=>'Potion', 326=>'Print', 327=>'PrintMix', 328=>'Pumice', 329=>'Purple', 330=>'Quartz',
        331=>'Raven', 332=>'Red', 333=>'RedMulti', 334=>'RedRose', 335=>'Red/Black', 336=>'Red/White', 337=>'RoseRed',
        338=>'Rosewood', 339=>'Royal', 340=>'RoyalPony', 341=>'Safari', 342=>'Sapphire', 343=>'Sateen', 344=>'Satellite',
        345=>'Scarlet', 346=>'Scuba', 347=>'Shadow', 348=>'Silver', 349=>'SilverMarle', 350=>'Silver/Gold', 351=>'Skeleton',
        352=>'SkyPony', 353=>'Smoke', 354=>'Smoke/Black', 355=>'Smoke/Green', 356=>'Soap', 357=>'Steel', 358=>'Steel/Black',
        359=>'Steel/Sil', 360=>'String', 361=>'Stripe/Black', 362=>'T-Shell', 363=>'Tar', 364=>'Tartan', 365=>'Thunder',
        366=>'Tidal', 367=>'Tortoise', 368=>'Truffle', 369=>'Tweed', 370=>'U/Pewt', 371=>'UtilityGreen', 372=>'UtilityGrn',
        373=>'Utility/Pk', 374=>'Vamp', 375=>'VintageBlack', 376=>'White', 377=>'White/Black', 378=>'White/Blue',
        379=>'White/Green', 380=>'White/Multi', 381=>'White/Navy', 382=>'White/Red', 383=>'Whitewash', 384=>'Yellow',
        385=>'ZambesiBlack', 386=>'BlackDiamond', 387=>'BlackDiamond/Blk', 388=>'Black/Pumice', 389=>'BottlePrism',
        390=>'BottlePrism/Black', 391=>'Burgundy/Dusk', 392=>'Burgundy/Red', 393=>'DiamondMixPrint', 394=>'Dusk/Storm',
        395=>'GreenDiamond', 396=>'Green/Green', 397=>'Green/Storm', 398=>'Orange/Pumice', 399=>'Orange/Red',
        400=>'PrismMixPrint', 401=>'RedPrism', 402=>'Red/Orange', 403=>'Storm', 404=>'Navy/Bleach', 405=>'Navy/Yellow',
        406=>'Plaid', 407=>'Grey/Black', 408=>'Grey/Burgundy', 409=>'Stripe', 410=>'BlackRussian', 411=>'Blood', 412=>'Tear',
        13=>'BlkRhodium', 414=>'OxidisedSilver', 415=>'9ctGold', 416=>'Glass/Silver', 417=>'Resin/Petals', 418=>'SterlingSilver',
        419=>'Blue/White', 420=>'Black/Print', 421=>'BlueSlate', 422=>'BlueSlate/Black', 423=>'CharcoalMarle',
        424=>'Indigo/Black', 425=>'Inkpen', 426=>'Inkpen/Black', 427=>'Licorice', 428=>'Licorice/Black', 429=>'Licorice/Steel',
        430=>'Oil', 431=>'Stone', 432=>'Stone/Black', 433=>'NavyStripe', 434=>'Ashphalt/Tarseal', 435=>'Black/Cream', 436=>'Coal',
        437=>'Electric/Tarseal', 438=>'Jetsam', 439=>'Mauve', 440=>'Mauve/Cream', 441=>'Navy/Cream', 442=>'Neo', 443=>'Vanilla',
        444=>'Violet', 445=>'Bone', 446=>'Graphite', 447=>'Rust', 448=>'Spec', 449=>'Syrah', 450=>'10K/Diamond',
        451=>'10K/Emerald', 452=>'10K/Ruby', 453=>'10K/Silver/Dia', 454=>'10K/Silver/Ruby', 455=>'18K', 456=>'Silver/Emerald',
        457=>'Stars', 458=>'Stripe/DarkGrey', 459=>'Stripe/Khaki', 460=>'BlackSpots', 461=>'BlueSpot', 462=>'PatternBlack',
        463=>'Frostbite', 464=>'Uzi', 465=>'Black/Natural', 466=>'Natural/Black', 467=>'Bonfire', 468=>'Gothic', 469=>'Lotus',
        470=>'PaleBlue', 471=>'DarkDust', 472=>'Rose', 473=>'BlackScrub', 474=>'Black/Navy', 475=>'Blk/Blk/Floral',
        476=>'Cream/Black', 477=>'Graphic/Yellow', 478=>'Gingham/Black', 479=>'Ballet', 480=>'BasicBlack', 481=>'BlackEmblem',
        482=>'BlackEyelet', 483=>'BlackVeil', 484=>'Black/Nickel', 485=>'Blacklawn', 486=>'Blacksand', 487=>'Crystal',
        488=>'Decoritif', 489=>'Emblem', 490=>'Fog', 491=>'InkTattoo', 492=>'Ivory', 493=>'Jet', 494=>'KholTattoo',
        495=>'Marshmellow', 496=>'Mesh', 497=>'Mist', 498=>'NavyEmblem', 499=>'Porcelain', 500=>'Saphire', 501=>'SilverEyelet',
        502=>'Taupe', 503=>'ThinStripe', 504=>'TripleStripe', 505=>'WhiteEmblem', 506=>'WhiteEyelet', 507=>'WhiteVeil',
        508=>'Whitelawn', 509=>'Anthracite', 510=>'BlackHAHA', 511=>'BlackPutty', 512=>'Dove', 513=>'HAHA', 514=>'HAHAX',
        515=>'Nude', 516=>'Putty', 517=>'PuttyBlack', 518=>'RedSlate', 519=>'Slate', 520=>'X', 521=>'Green/White',
        522=>'LightOrange', 523=>'Orange/Pink', 524=>'Pink/Yellow', 525=>'Yellow/Orange', 526=>'DarkDusk', 527=>'Dust',
        528=>'DarkAnimal', 529=>'LightAnimal', 530=>'Black/Stripe', 531=>'Ecru/Black', 532=>'Grey/Green', 533=>'RedStripe',
        534=>'GreenStripe', 535=>'14ctGold', 536=>'Concrete', 537=>'Black/Blush', 538=>'BlackStripe', 539=>'Black/BlackPrint',
        540=>'Black/Gothic', 541=>'Black/Lurex', 542=>'Black/Milk', 543=>'Black/PinkPrint', 544=>'Black/Wallpaper',
        545=>'RedCheck', 546=>'Wallpaper', 547=>'Black/Putty', 548=>'Chambray', 549=>'Ashes', 550=>'BasicNavy', 551=>'BlackV',
        552=>'Blackfleece', 553=>'Blackwood', 554=>'Brushblack', 555=>'Cloud', 556=>'Coaldust', 557=>'Dark', 558=>'DarkMix',
        559=>'Drill', 560=>'Faux', 561=>'FineBlack', 562=>'Garnet', 563=>'Khol', 564=>'Labyrinth', 565=>'NavyFleece',
        566=>'PaleMix', 567=>'Polish', 568=>'Ruby', 569=>'Spotlight', 570=>'Thunderbird', 571=>'YellowFleece',
        572=>'BlueStripes', 573=>'FluroYellow', 574=>'NavyDots', 575=>'Orange/Print', 576=>'Black/Brown', 577=>'Navy/Stripe',
        578=>'Pattern/Black', 579=>'Peach', 580=>'LightBlue', 581=>'Turquoise', 582=>'Chocolate', 583=>'OffWhite',
        584=>'RainyMorning', 585=>'Black/Almond', 586=>'Palm', 587=>'BlackAngel', 588=>'BlackMix', 589=>'Chalk', 590=>'ClayCheck',
        591=>'Covered', 592=>'DarkInk', 593=>'Flower', 594=>'InkAngel', 595=>'InkMix', 596=>'OliveAngel', 597=>'OliveMix',
        598=>'Sand', 599=>'Skin', 600=>'Tapestry', 601=>'Lurex', 602=>'Alligator', 603=>'BasicGrey', 604=>'BlackFoil',
        605=>'Blackadder', 606=>'Blackbird', 607=>'Charred', 608=>'Eclipse', 609=>'Emerald', 610=>'Greenacres', 611=>'Iceberg',
        612=>'Lacquer', 613=>'Natural', 614=>'Negative', 615=>'Phantom', 616=>'Positive', 617=>'Smudge', 618=>'Volcanic',
        619=>'WhiteDove', 620=>'Zinc');

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

        if ($entityType != 'product') {
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
                $this->attributeSets[$attributeSet->attribute_set_id] = (array) $attributeSet;
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
     * @throws MagelinkException
     * @throws NodeException
     * @throws SyncException
     * @throws GatewayException
     */
    public function retrieveEntities()
    {
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
     * Restructure data for rest call and return this array.
     * @param array $data
     * @param array $customAttributes
     * @return array $restData
     * @throws \Magelink\Exception\MagelinkException
     */
    protected function getUpdateDataForSoapCall(array $data, array $customAttributes)
    {
        // Restructure data for rest call
        $restData = array(
            'additional_attributes'=>array(
                'single_data'=>array(),
                'multi_data'=>array()
            )
        );
        $removeSingleData = $removeMultiData = TRUE;

        foreach ($data as $code=>$value) {
            $isCustomAttribute = in_array($code, $customAttributes);
            if ($isCustomAttribute) {
                if (is_array($data[$code])) {
                    // TECHNICAL DEBT // ToDo(maybe) : Implement
                    throw new GatewayException("This gateway doesn't support multi_data custom attributes yet.");
                    $removeMultiData = FALSE;
                }else{
                    $restData['additional_attributes']['single_data'][] = array(
                        'key'=>$code,
                        'value'=>$value,
                    );
                    $removeSingleData = FALSE;
                }
            }else{
                $restData[$code] = $value;
            }
        }

        if ($removeSingleData) {
            unset($data['additional_attributes']['single_data']);
        }
        if ($removeMultiData) {
            unset($data['additional_attributes']['multi_data']);
        }
        if ($removeSingleData && $removeMultiData) {
            unset($data['additional_attributes']);
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
               array('entity'=>$entity)
            );

        $originalData = $entity->getFullArrayCopy();
        $attributeCodes = array_unique(array_merge(
            //array('special_price', 'special_from_date', 'special_to_date'), // force update of these attributes
            //$customAttributes,
            $attributes
        ));

        foreach ($originalData as $attributeCode=>$attributeValue) {
            if (!in_array($attributeCode, $attributeCodes)) {
                unset($originalData[$attributeCode]);
            }
        }

        $data = array();
        if (count($originalData) == 0) {
            $this->getServiceLocator()->get('logService')
                ->log(LogService::LEVEL_INFO,
                    $this->getLogCode().'_wrupd_non',
                    'No update required for '.$sku.' but requested was '.implode(', ', $attributes),
                    array('attributes'=>$attributes),
                    array('entity'=>$entity)
                );
        }else{
            /** @var Magento2Service $magento2Service */
            $magento2Service = $this->getServiceLocator()->get('magento2Service');

            foreach ($originalData as $code=>$value) {
                $mappedCode = $magento2Service->getMappedCode('product', $code);
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
                        // Ignore attributes
                        break;
                    case 'product_class':
                    case 'type':
                        if ($type != Update::TYPE_CREATE) {
                            // TECHNICAL DEBT // ToDo: Log error(but no exception)
                        }else{
                            // Ignore attributes
                        }
                        break;
                    default:
                        $this->getServiceLocator()->get('logService')
                            ->log(LogService::LEVEL_WARN,
                                $this->getLogCode().'_wr_invdata',
                                'Unsupported attribute for update of '.$sku.': '.$attributeCode,
                               array('attribute'=>$attributeCode),
                               array('entity'=>$entity)
                            );
                        // Warn unsupported attribute
                }
            }

            $localId = $this->_entityService->getLocalId($this->_node->getNodeId(), $entity);

            $storeDataByStoreId = $this->_node->getStoreViews();
            if (count($storeDataByStoreId) > 0 && $type != Update::TYPE_DELETE) {
                $dataPerStore[0] = $data;
                foreach (array('price', 'special_price', 'msrp', 'cost') as $code) {
                    if (array_key_exists($code, $data)) {
                        unset($data[$code]);
                    }
                }

                $websiteIds = array();
                foreach ($storeDataByStoreId as $storeId=>$storeData) {
                    $dataToMap = $magento2Service->mapProductData($data, $storeId, FALSE, TRUE);
                    if ($magento2Service->isStoreUsingDefaults($storeId)) {
                        $dataToCheck = $dataPerStore[0];
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
                unset($data, $dataToMap, $dataToCheck);

                $storeIds = array_merge(array(0), array_keys($storeDataByStoreId));
                $this->getServiceLocator()->get('logService')->log(LogService::LEVEL_DEBUGINTERNAL,
                    $this->getLogCode().'_wrupd_stor',
                    'StoreIds '.json_encode($storeIds).' (type: '.$type.'), websiteIds '.json_encode($websiteIds).'.',
                    array('store data'=>$storeDataByStoreId)
                );

                foreach ($storeIds as $storeId) {
                    $productData = $dataPerStore[$storeId];
                    $productData['website_ids'] = $websiteIds;

                    if ($magento2Service->isStoreUsingDefaults($storeId)) {
                        $setSpecialPrice = FALSE;
                        unset($productData['special_price']);
                        unset($productData['special_from_date']);
                        unset($productData['special_to_date']);
                    }elseif (isset($productData['special_price'])) {
                        $setSpecialPrice = FALSE;
                    }elseif ($storeId === 0) {
                        $setSpecialPrice = FALSE;
                        $productData['special_price'] = NULL;
                        $productData['special_from_date'] = NULL;
                        $productData['special_to_date'] = NULL;
                    }else{
                        $setSpecialPrice = FALSE;
                        $productData['special_price'] = '';
                        $productData['special_from_date'] = '';
                        $productData['special_to_date'] = '';
                    }
// ToDo: Change to Rest
                    $restData = $this->getUpdateDataForSoapCall($productData, $customAttributes);
                    $logData = array(
                        'type'=>$entity->getData('type'),
                        'store id'=>$storeId,
                        'product data'=>$productData,
                    );
                    $restResult = NULL;

                    $updateViaDbApi = ($this->restV1 && $localId && $storeId == 0);
                    if ($updateViaDbApi) {
                        $api = 'DB';
                    }else{
                        $api = 'REST';
                    }

                    if ($type == Update::TYPE_UPDATE || $localId) {
                        if ($updateViaDbApi) {
                            try{
                                $tablePrefix = 'catalog_product';
                                $rowsAffected = $this->db->updateEntityEav(
                                    $tablePrefix,
                                    $localId,
                                    $entity->getStoreId(),
                                    $productData
                                );

                                if ($rowsAffected != 1) {
                                    throw new MagelinkException($rowsAffected.' rows affected.');
                                }
                            }catch(\Exception $exception) {
                                $this->_entityService->unlinkEntity($nodeId, $entity);
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
                                $putData = $restData; // $sku, $storeId
                                if ($setSpecialPrice) {
                                    $putData['custom_attributes'][] = array(
                                        'attribute_code'=>'special_price',
                                        'value'=>$productData['special_price']
                                    );
                                    $putData['custom_attributes'][] = array(
                                        'attribute_code'=>'special_price_from_date',
                                        'value'=>$productData['special_from_date']
                                    );
                                    $putData['custom_attributes'][] = array(
                                        'attribute_code'=>'special_price',
                                        'value'=>$productData['special_to_date']
                                    );
                                }

                                $restResult = array('update'=>
                                    $this->restV1->put('products/'.$sku, $putData));
                            }catch(\Exception $exception) {
                                $restResult = FALSE;
                                if (is_null($exception->getPrevious())) {
                                    $restFaultMessage = $exception->getMessage();
                                }else{
                                    $restFaultMessage = $exception->getPrevious()->getMessage();
                                }
                                if (strpos($restFaultMessage, 'Product not exists') !== FALSE) {
                                    $type = Update::TYPE_CREATE;
                                }
                            }

                            $logLevel = ($restResult ? LogService::LEVEL_INFO : LogService::LEVEL_ERROR);
                            $logCode = $this->getLogCode().'_wrupdrest';
                            if ($api != 'SOAP') {
                                $logMessage = $api.' update failed. Removed local id '.$localId
                                    .' from node '.$nodeId.'. '.$logMessage;
                                if (isset($exception)) {
                                    $logData[strtolower($api.' error')] = $exception->getMessage();
                                }
                            }

                            $logMessage .= ($restResult ? 'successfully' : 'without success').' via SOAP api.'
                                .($type == Update::TYPE_CREATE ? ' Try to create now.' : '');
                            $logData['rest data'] = $restData;
                            $logData['rest result'] = $restResult;
                        }
                        $this->getServiceLocator()->get('logService')->log($logLevel, $logCode, $logMessage, $logData);
                    }

                    if ($type == Update::TYPE_CREATE) {
                        $attributeSet = NULL;
                        foreach ($this->attributeSets as $setId=>$set) {
                            if ($set['name'] == $entity->getData('product_class', 'default')) {
                                $attributeSet = $setId;
                                break;
                            }
                        }
                        if ($attributeSet === NULL) {
                            $message = 'Invalid product class '.$entity->getData('product_class', 'default');
                            throw new \Magelink\Exception\SyncException($message);
                        }

                        $message = 'Creating product (REST) : '.$sku.' with '.implode(', ', array_keys($productData));
                        $logData['set'] = $attributeSet;
                        $this->getServiceLocator()->get('logService')
                            ->log(LogService::LEVEL_INFO, $this->getLogCode().'_wr_cr', $message, $logData);

                        $request = array(
                            $entity->getData('type'),
                            $attributeSet,
                            $sku,
                            $restData,
                            $entity->getStoreId()
                        );

                        try{
                            $restResult = $this->restV1->post('products', $restData);
                            $restFault = NULL;
                        }catch(\Exception $exception) {
                            $restResult = FALSE;
                            $restFault = $exception->getPrevious();
                            $restFaultMessage = $restFault->getMessage();
                            if ($restFaultMessage == 'The value of attribute "SKU" must be unique') {
                                $this->getServiceLocator()->get('logService')
                                    ->log(LogService::LEVEL_WARN,
                                        $this->getLogCode().'_wr_duperr',
                                        'Creating product '.$sku.' hit SKU duplicate fault',
                                        array(),
                                        array('entity'=>$entity, 'rest fault'=>$restFault)
                                    );

                                $check = $this->restV1->get('products/'.$sku, array());
                                if (!$check || !count($check)) {
                                    throw new MagelinkException(
                                        'Magento2 complained duplicate SKU but we cannot find a duplicate!'
                                    );

                                }else{
                                    $found = FALSE;
                                    foreach ($check as $row) {
                                        if ($row['sku'] == $sku) {
                                            $found = TRUE;

                                            $this->_entityService->linkEntity($nodeId, $entity, $row['product_id']);
                                            $this->getServiceLocator()->get('logService')
                                                ->log(LogService::LEVEL_INFO,
                                                    $this->getLogCode().'_wr_dupres',
                                                    'Creating product '.$sku.' resolved SKU duplicate fault',
                                                    array('local_id'=>$row['product_id']),
                                                    array('entity'=>$entity)
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
                            $this->_entityService->linkEntity($nodeId, $entity, $restResult);
                            $type = Update::TYPE_UPDATE;

                            $logData['rest data'] = $restData;
                            $this->getServiceLocator()->get('logService')->log(LogService::LEVEL_INFO,
                                $this->getLogCode().'_wr_loc_id',
                                'Added product local id '.$restResult.' for '.$sku.' ('.$nodeId.')',
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
