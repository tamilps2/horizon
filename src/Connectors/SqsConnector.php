<?php

namespace Laravel\Horizon\Connectors;

use Illuminate\Queue\Connectors\SqsConnector as BaseConnector;
use Laravel\Horizon\SqsQueue;
use Aws\Sqs\SqsClient;
use Illuminate\Support\Arr;

class SqsConnector extends BaseConnector
{

    public function connect(array $config)
    {
        $config = $this->getDefaultConfiguration($config);

        if (! empty($config['key']) && ! empty($config['secret'])) {
            $config['credentials'] = Arr::only($config, ['key', 'secret', 'token']);
        }

        return new SqsQueue(
            new SqsClient(
                Arr::except($config, ['token'])
            ),
            $config['queue'],
            Arr::get($config, 'prefix', ''),
            Arr::get($config, 'suffix', ''),
            Arr::get($config, 'after_commit', null)
        );
    }
}
