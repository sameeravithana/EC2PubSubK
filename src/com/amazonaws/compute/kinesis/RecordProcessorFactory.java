package com.amazonaws.compute.kinesis;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;

public class RecordProcessorFactory implements IRecordProcessorFactory {

	/**
     * Constructor.
     */
    public RecordProcessorFactory() {
        super();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IRecordProcessor createProcessor() {
        return new RecordProcessor();
    }
}
