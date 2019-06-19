FROM lambci/lambda:build-python3.7

WORKDIR /var/task
ENV WORKDIR /var/task
ENV LAMBDANAME gfw-glad-pipeline

RUN mkdir -p packages/ \
    && pip install retrying boto3 google-cloud-storage -t packages

#Precompile all python packages and remove .py files
#RUN python -m compileall .
#RUN find packages/ -type f -name '*.pyc' | while read f; do n=$(echo $f | sed 's/__pycache__\///' | sed 's/.cpython-37//'); cp $f $n; done;
#RUN find packages/ -type d -a -name '__pycache__' -print0 | xargs -0 rm -rf
#RUN find packages/ -type f -a -name '*.py' -print0 | xargs -0 rm -f

# Copy initial source codes into container.
COPY lambda_function.py "$WORKDIR/lambda_function.py"
COPY bootstrap.sh "$WORKDIR/bootstrap.sh"
COPY .lambdaignore "$WORKDIR/packages/.lambdaignore"

# Compress all source codes expect files listed in .lambdaignore
RUN cd $WORKDIR/packages && \
#    cat .lambdaignore | xargs zip -9qyr $WORKDIR/lambda.zip . -x
     zip -9qyr $WORKDIR/lambda.zip .

RUN cd $WORKDIR && \
    zip -g lambda.zip lambda_function.py && \
    zip -g lambda.zip bootstrap.sh

#CMD ["/bin/bash"]
CMD aws lambda update-function-code --function-name $LAMBDANAME --zip-file fileb://lambda.zip
#CMD aws lambda create-function --function-name $LAMBDANAME \
#                                --runtime python3.7 \
#                                --role arn:aws:iam::838255262149:role/gfw_lambda_glad_trigger \
#                                --handler lambda_function.lambda_handler \
#                                --zip-file fileb://lambda.zip \
#                                --description "Trigger for GFW GLAD Pipeline" \
#                                --no-publish \
#                                --tags "Project=Global Forest Watch"
#                                --timeout 90

# docker build -t gfw/gfw-glad-pipeline .
# docker run --rm -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_DEFAULT_REGION  gfw/gfw-glad-pipeline